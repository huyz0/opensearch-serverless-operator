/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.serverless.operator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.Transport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.AlgorithmParameters;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.EdECPublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.HexFormat;

/** Startup-only peer mTLS. This does not secure HTTP or authorize peer actions. */
public final class PeerTlsPlugin extends Plugin implements SecureSettingsFactory, SecureTransportSettingsProvider {
    private static final Setting<String> CERT = Setting.simpleString("serverless.peer_tls.certificate", Setting.Property.NodeScope);
    private static final Setting<String> KEY = Setting.simpleString("serverless.peer_tls.private_key", Setting.Property.NodeScope);
    private static final Setting<String> TRUST = Setting.simpleString("serverless.peer_tls.trust", Setting.Property.NodeScope);
    private static final Setting<String> EXPECTED_LEAF = Setting.simpleString("serverless.peer_tls.expected_leaf_sha256", Setting.Property.NodeScope);
    private static final Setting<String> EXPECTED_TRUST = Setting.simpleString("serverless.peer_tls.expected_trust_sha256", Setting.Property.NodeScope);
    private final SslContext server;
    private final SslContext client;

    public PeerTlsPlugin(Settings settings) throws SSLException {
        String type = settings.get("transport.type", Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME);
        if (Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME.equals(type) == false
            || settings.getAsBoolean("transport.ssl.enforce_hostname_verification", true) == false
            || settings.getAsBoolean("transport.ssl.dual_mode.enabled", false)) {
            throw new IllegalArgumentException("peer TLS requires secure transport, hostname verification, and no dual mode");
        }
        File cert = requiredFile(CERT.get(settings));
        File key = requiredFile(KEY.get(settings));
        File trust = requiredFile(TRUST.get(settings));
        byte[] keyBytes = null;
        try {
            byte[] certificateBytes = Files.readAllBytes(cert.toPath());
            keyBytes = Files.readAllBytes(key.toPath());
            byte[] trustBytes = Files.readAllBytes(trust.toPath());
            KeyMaterial material = verifyKeyPair(certificateBytes, keyBytes);
            String expectedLeaf = EXPECTED_LEAF.get(settings);
            String expectedTrust = EXPECTED_TRUST.get(settings);
            if (expectedLeaf.isEmpty() == false || expectedTrust.isEmpty() == false) {
                if (expectedLeaf.matches("[0-9a-f]{64}") == false || expectedTrust.matches("[0-9a-f]{64}") == false) {
                    throw new SSLException("both public peer TLS hashes must be supplied");
                }
                MessageDigest sha = MessageDigest.getInstance("SHA-256");
                if (expectedLeaf.equals(HexFormat.of().formatHex(sha.digest(material.chain()[0].getEncoded()))) == false
                    || expectedTrust.equals(HexFormat.of().formatHex(sha.digest(trustBytes))) == false) {
                    throw new SSLException("mounted peer TLS material differs from the approved revision");
                }
            }
            server = SslContextBuilder.forServer(material.key(), material.chain())
                .trustManager(new ByteArrayInputStream(trustBytes)).clientAuth(ClientAuth.REQUIRE).build();
            client = SslContextBuilder.forClient().keyManager(material.key(), material.chain())
                .trustManager(new ByteArrayInputStream(trustBytes)).build();
        } catch (Exception e) {
            throw new SSLException("invalid peer TLS material", e);
        } finally {
            if (keyBytes != null) Arrays.fill(keyBytes, (byte) 0);
        }
    }

    record KeyMaterial(PrivateKey key, X509Certificate[] chain) {}

    static KeyMaterial verifyKeyPair(byte[] certificateBytes, byte[] keyBytes) throws Exception {
        X509Certificate[] chain = CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(certificateBytes))
            .stream().map(cert -> (X509Certificate) cert).toArray(X509Certificate[]::new);
        if (chain.length == 0) throw new SSLException("empty peer certificate chain");
        X509Certificate certificate = chain[0];
        certificate.checkValidity();
        String algorithm = certificate.getPublicKey().getAlgorithm();
        String signatureAlgorithm;
        if ("RSA".equalsIgnoreCase(algorithm)) signatureAlgorithm = "SHA256withRSA";
        else if ("EC".equalsIgnoreCase(algorithm)) signatureAlgorithm = "SHA256withECDSA";
        else if (certificate.getPublicKey() instanceof EdECPublicKey edKey) {
            algorithm = edKey.getParams().getName();
            signatureAlgorithm = algorithm;
        } else throw new SSLException("unsupported peer certificate public key algorithm");

        String pem = new String(keyBytes, StandardCharsets.US_ASCII).trim();
        String label;
        if (pem.startsWith("-----BEGIN PRIVATE KEY-----")) label = "PRIVATE KEY";
        else if (pem.startsWith("-----BEGIN RSA PRIVATE KEY-----") && "RSA".equalsIgnoreCase(algorithm)) label = "RSA PRIVATE KEY";
        else if (pem.startsWith("-----BEGIN EC PRIVATE KEY-----") && "EC".equalsIgnoreCase(algorithm)) label = "EC PRIVATE KEY";
        else throw new SSLException("expected an unencrypted PKCS#8, RSA PKCS#1 or EC SEC1 private key");
        String begin = "-----BEGIN " + label + "-----";
        String end = "-----END " + label + "-----";
        if (pem.endsWith(end) == false) throw new SSLException("invalid private key PEM");
        byte[] raw = Base64.getDecoder().decode(pem.substring(begin.length(), pem.length() - end.length()).replaceAll("\\s", ""));
        byte[] encoded = raw;
        try {
            if (label.equals("RSA PRIVATE KEY")) {
                byte[] rsaAlgorithm = new byte[]{0x30, 0x0d, 0x06, 0x09, 0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00};
                encoded = pkcs8(raw, rsaAlgorithm);
            } else if (label.equals("EC PRIVATE KEY")) {
                AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
                parameters.init(((ECPublicKey) certificate.getPublicKey()).getParams());
                byte[] oid = new byte[]{0x06, 0x07, 0x2a, (byte) 0x86, 0x48, (byte) 0xce, 0x3d, 0x02, 0x01};
                encoded = pkcs8(raw, der(0x30, concat(oid, parameters.getEncoded())));
            }
            PrivateKey privateKey = KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(encoded));
            Signature signer = Signature.getInstance(signatureAlgorithm);
            byte[] challenge = "opensearch-serverless-peer-tls-key-check".getBytes(StandardCharsets.US_ASCII);
            signer.initSign(privateKey);
            signer.update(challenge);
            byte[] signature = signer.sign();
            signer.initVerify(certificate.getPublicKey());
            signer.update(challenge);
            if (signer.verify(signature) == false) throw new SSLException("peer certificate and private key do not match");
            return new KeyMaterial(privateKey, chain);
        } finally {
            Arrays.fill(encoded, (byte) 0);
            Arrays.fill(raw, (byte) 0);
        }
    }

    private static byte[] concat(byte[]... values) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        for (byte[] value : values) result.writeBytes(value);
        return result.toByteArray();
    }

    private static byte[] der(int tag, byte[] value) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        result.write(tag);
        if (value.length < 128) result.write(value.length);
        else {
            int length = value.length;
            int bytes = (32 - Integer.numberOfLeadingZeros(length) + 7) / 8;
            result.write(0x80 | bytes);
            for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) result.write((length >>> shift) & 0xff);
        }
        result.writeBytes(value);
        return result.toByteArray();
    }

    private static byte[] pkcs8(byte[] key, byte[] algorithmIdentifier) {
        return der(0x30, concat(new byte[]{0x02, 0x01, 0x00}, algorithmIdentifier, der(0x04, key)));
    }

    static File requiredFile(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("peer TLS file path is required");
        File file = new File(name);
        if (file.isAbsolute() == false || file.isFile() == false) throw new IllegalArgumentException("peer TLS file must exist and be absolute");
        return file;
    }

    @Override public List<Setting<?>> getSettings() { return List.of(CERT, KEY, TRUST, EXPECTED_LEAF, EXPECTED_TRUST); }
    @Override public Settings additionalSettings() {
        return Settings.builder().put("transport.type", Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME)
            .put("transport.ssl.enforce_hostname_verification", true).put("transport.ssl.resolve_hostname", false)
            .put("transport.ssl.dual_mode.enabled", false).build();
    }
    @Override public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) { return Optional.of(this); }
    @Override public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) { return Optional.of(this); }
    @Override public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) { return Optional.empty(); }
    @Override public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) { return Optional.empty(); }
    @Override public Optional<TransportExceptionHandler> buildServerTransportExceptionHandler(Settings settings, Transport transport) { return Optional.empty(); }
    @Override public Optional<SSLEngine> buildSecureServerTransportEngine(Settings settings, Transport transport) {
        SSLEngine engine = server.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(false);
        engine.setNeedClientAuth(true);
        return Optional.of(engine);
    }
    @Override public Optional<SSLEngine> buildSecureClientTransportEngine(Settings settings, String hostname, int port) throws SSLException {
        if (hostname == null || hostname.isBlank() || port <= 0) throw new SSLException("peer address required for identity verification");
        SSLEngine engine = client.newEngine(ByteBufAllocator.DEFAULT, hostname, port);
        engine.setUseClientMode(true);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(parameters);
        return Optional.of(engine);
    }
}
