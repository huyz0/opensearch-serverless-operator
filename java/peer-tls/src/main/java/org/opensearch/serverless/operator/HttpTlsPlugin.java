/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.serverless.operator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SecureAuxTransportSettingsProvider;
import org.opensearch.plugins.SecureHttpTransportSettingsProvider;
import org.opensearch.plugins.SecureSettingsFactory;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.plugins.TransportExceptionHandler;
import org.opensearch.transport.Netty4ModulePlugin;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/** Startup-only HTTPS provider. Authentication and authorization are separate. */
public final class HttpTlsPlugin extends Plugin implements SecureSettingsFactory, SecureHttpTransportSettingsProvider {
    private static final Setting<String> CERT = Setting.simpleString("serverless.http_tls.certificate", Setting.Property.NodeScope);
    private static final Setting<String> KEY = Setting.simpleString("serverless.http_tls.private_key", Setting.Property.NodeScope);
    private static final Setting<String> EXPECTED_CERT = Setting.simpleString("serverless.http_tls.expected_certificate_sha256", Setting.Property.NodeScope);
    private static final Setting<String> TRUST = Setting.simpleString("serverless.http_tls.client_trust", Setting.Property.NodeScope);
    private static final Setting<Boolean> CLIENT_AUTH = Setting.boolSetting("serverless.http_tls.require_client_certificate", false, Setting.Property.NodeScope);
    private final SslContext server;
    private final boolean requireClient;

    public HttpTlsPlugin(Settings settings) throws SSLException {
        if (Netty4ModulePlugin.NETTY_SECURE_HTTP_TRANSPORT_NAME.equals(settings.get("http.type", Netty4ModulePlugin.NETTY_SECURE_HTTP_TRANSPORT_NAME)) == false) {
            throw new IllegalArgumentException("HTTP TLS requires the secure HTTP transport");
        }
        requireClient = CLIENT_AUTH.get(settings);
        byte[] keyBytes = null;
        try {
            byte[] cert = Files.readAllBytes(PeerTlsPlugin.requiredFile(CERT.get(settings)).toPath());
            String expected = EXPECTED_CERT.get(settings);
            if (expected.isEmpty() == false && expected.equals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cert))) == false) {
                throw new IllegalArgumentException("HTTP TLS certificate differs from the expected public material");
            }
            keyBytes = Files.readAllBytes(PeerTlsPlugin.requiredFile(KEY.get(settings)).toPath());
            PeerTlsPlugin.KeyMaterial material = PeerTlsPlugin.verifyKeyPair(cert, keyBytes);
            SslContextBuilder builder = SslContextBuilder.forServer(material.key(), material.chain());
            if (requireClient) {
                byte[] trust = Files.readAllBytes(PeerTlsPlugin.requiredFile(TRUST.get(settings)).toPath());
                builder.trustManager(new ByteArrayInputStream(trust)).clientAuth(ClientAuth.REQUIRE);
            } else {
                builder.clientAuth(ClientAuth.NONE);
            }
            server = builder.build();
        } catch (Exception e) {
            throw new SSLException("invalid HTTP TLS material", e);
        } finally {
            if (keyBytes != null) Arrays.fill(keyBytes, (byte) 0);
        }
    }

    @Override public List<Setting<?>> getSettings() { return List.of(CERT, KEY, EXPECTED_CERT, TRUST, CLIENT_AUTH); }
    @Override public Settings additionalSettings() { return Settings.builder().put("http.type", Netty4ModulePlugin.NETTY_SECURE_HTTP_TRANSPORT_NAME).build(); }
    @Override public Optional<SecureSettingsFactory> getSecureSettingFactory(Settings settings) { return Optional.of(this); }
    @Override public Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings) { return Optional.empty(); }
    @Override public Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings) { return Optional.of(this); }
    @Override public Optional<SecureAuxTransportSettingsProvider> getSecureAuxTransportSettingsProvider(Settings settings) { return Optional.empty(); }
    @Override public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport) { return Optional.empty(); }
    @Override public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) {
        SSLEngine engine = server.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(false);
        engine.setNeedClientAuth(requireClient);
        return Optional.of(engine);
    }
}
