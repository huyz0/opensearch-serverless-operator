/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.serverless.operator;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

public class PeerTlsIntegrationTests extends OpenSearchTestCase {
    private Settings settings(String name, String certificate, String trust) {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        return Settings.builder().put("node.name", name).put("cluster.name", "operator-peer-tls")
            .put("path.home", createTempDir()).put("network.host", "127.0.0.1").put("http.port", "0")
            .put("transport.port", "0").put("serverless.peer_tls.certificate", fixtures.resolve(certificate))
            .put("serverless.peer_tls.private_key", fixtures.resolve("node.key"))
            .put("serverless.peer_tls.trust", fixtures.resolve(trust)).put("transport.ssl.resolve_hostname", false).build();
    }

    private void forwardedWrite(String ownerCert, String clientTrust, boolean succeeds) throws Exception {
        Settings ownerSettings = settings("owner", ownerCert, "ca.crt");
        Settings peerSettings = settings("peer", "node.crt", clientTrust);
        AtomicLong clock = new AtomicLong(1000);
        MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30000);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        try (ServerlessNode owner = new ServerlessNode(ownerSettings, List.of(new PeerTlsPlugin(ownerSettings)));
             ServerlessNode peer = new ServerlessNode(peerSettings, List.of(new PeerTlsPlugin(peerSettings)))) {
            owner.startup().start();
            peer.startup().start();
            owner.setMetadataPlane(plane);
            peer.setMetadataPlane(plane);
            owner.activation().activateWriter(plane, "alpha", 0);
            peer.activation().syncFrom(plane);
            assertTrue(peer.reconciler().openShards().isEmpty());
            if (succeeds) {
                peer.services().client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"tls\"}", XContentType.JSON)).actionGet();
                assertTrue(peer.services().client().get(new GetRequest("alpha", "k")).actionGet().isExists());
            } else {
                expectThrows(Exception.class, () -> peer.services().client()
                    .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"tls\"}", XContentType.JSON)).actionGet());
                assertFalse(owner.services().client().get(new GetRequest("alpha", "k")).actionGet().isExists());
            }
        }
    }

    public void testForwardingWithVerifiedPeer() throws Exception { forwardedWrite("node.crt", "ca.crt", true); }
    public void testWrongServerIdentityIsRejected() throws Exception { forwardedWrite("wrong.crt", "ca.crt", false); }
    public void testUntrustedServerIsRejected() throws Exception { forwardedWrite("node.crt", "other-ca.crt", false); }

    private SSLContext clientContext(String name, String trustFile) throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        try (InputStream input = Files.newInputStream(fixtures.resolve(trustFile))) {
            int index = 0;
            for (var certificate : factory.generateCertificates(input)) {
                trust.setCertificateEntry("ca-" + index++, certificate);
            }
        }
        TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(trust);
        KeyManagerFactory km = null;
        if (name != null) {
            KeyStore keys = KeyStore.getInstance(KeyStore.getDefaultType());
            keys.load(null, null);
            String pem = Files.readString(fixtures.resolve(name + ".key"))
                .replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "");
            var key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(pem)));
            try (InputStream input = Files.newInputStream(fixtures.resolve(name + ".crt"))) {
                keys.setKeyEntry("client", key, new char[0], new java.security.cert.Certificate[]{factory.generateCertificate(input)});
            }
            km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            km.init(keys, new char[0]);
        }
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(km == null ? null : km.getKeyManagers(), tm.getTrustManagers(), null);
        return context;
    }

    private void clientHandshake(String credential, boolean accepted) throws Exception {
        Settings ownerSettings = settings("owner", "node.crt", "ca.crt");
        try (ServerlessNode owner = new ServerlessNode(ownerSettings, List.of(new PeerTlsPlugin(ownerSettings)))) {
            owner.startup().start();
            InetSocketAddress address = owner.localNode().getAddress().address();
            try (SSLSocket socket = (SSLSocket) clientContext(credential, "ca.crt").getSocketFactory().createSocket()) {
                socket.setEnabledProtocols(new String[]{"TLSv1.2"});
                socket.setSoTimeout(5000);
                socket.connect(address, 5000);
                if (accepted) socket.startHandshake();
                else expectThrows(SSLException.class, socket::startHandshake);
            }
        }
    }

    public void testTrustedClientCanHandshake() throws Exception { clientHandshake("node", true); }
    public void testClientWithoutCertificateIsRejected() throws Exception { clientHandshake(null, false); }
    public void testClientFromUntrustedCAIsRejected() throws Exception { clientHandshake("rogue", false); }

    public void testUnsafeTransportConfigurationIsRejected() {
        Settings valid = settings("unsafe", "node.crt", "ca.crt");
        for (String[] override : List.of(
            new String[]{"transport.type", "netty4"},
            new String[]{"transport.ssl.enforce_hostname_verification", "false"},
            new String[]{"transport.ssl.dual_mode.enabled", "true"},
            new String[]{"serverless.peer_tls.private_key", "relative.key"})) {
            Settings unsafe = Settings.builder().put(valid).put(override[0], override[1]).build();
            expectThrows(IllegalArgumentException.class, () -> new PeerTlsPlugin(unsafe));
        }
    }

    public void testMismatchedPrivateKeyIsRejectedAtStartup() {
        Settings base = settings("mismatch", "node.crt", "ca.crt");
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        Settings mismatch = Settings.builder().put(base).put("serverless.peer_tls.private_key", fixtures.resolve("rogue.key")).build();
        expectThrows(Exception.class, () -> new PeerTlsPlugin(mismatch));
    }

    public void testMountedPublicMaterialMustMatchApprovedHashes() throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        byte[] cert;
        try (InputStream in = Files.newInputStream(fixtures.resolve("node.crt"))) {
            cert = CertificateFactory.getInstance("X.509").generateCertificate(in).getEncoded();
        }
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        String leafHash = HexFormat.of().formatHex(sha.digest(cert));
        String trustHash = HexFormat.of().formatHex(sha.digest(Files.readAllBytes(fixtures.resolve("ca.crt"))));
        Settings base = settings("pinned", "node.crt", "ca.crt");
        Settings expected = Settings.builder().put(base).put("serverless.peer_tls.expected_leaf_sha256", leafHash)
            .put("serverless.peer_tls.expected_trust_sha256", trustHash).build();
        new PeerTlsPlugin(expected);
        for (Settings changed : List.of(
            Settings.builder().put(expected).put("serverless.peer_tls.certificate", fixtures.resolve("wrong.crt")).build(),
            Settings.builder().put(expected).put("serverless.peer_tls.trust", fixtures.resolve("other-ca.crt")).build(),
            Settings.builder().put(base).put("serverless.peer_tls.expected_leaf_sha256", leafHash).build())) {
            expectThrows(SSLException.class, () -> new PeerTlsPlugin(changed));
        }
    }

    public void testEcAndEd25519KeyPairsLoad() throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        for (String name : List.of("ec-node", "ed-node")) {
            Settings base = settings(name, name + ".crt", "ca.crt");
            Settings configured = Settings.builder().put(base).put("serverless.peer_tls.private_key", fixtures.resolve(name + ".key")).build();
            new PeerTlsPlugin(configured);
        }
    }

    public void testRsaPkcs1AndEcSec1LoadForPeerAndHttp() throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        for (String[] input : List.of(new String[]{"node.crt", "node-pkcs1.key"}, new String[]{"ec-node.crt", "ec-node-sec1.key"})) {
            Settings base = settings("legacy-key", input[0], "ca.crt");
            Settings configured = Settings.builder().put(base).put("serverless.peer_tls.private_key", fixtures.resolve(input[1]))
                .put("serverless.http_tls.certificate", fixtures.resolve(input[0]))
                .put("serverless.http_tls.private_key", fixtures.resolve(input[1])).build();
            try (ServerlessNode node = new ServerlessNode(configured, List.of(new PeerTlsPlugin(configured), new HttpTlsPlugin(configured)))) {
                node.startup().start();
                rotateHandshake(node.boundHttpAddress().publishAddress().address(), null, "ca.crt", true);
            }
        }
    }

    public void testRsaPkcs1AndEcSec1MismatchesAreRejected() {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        for (String[] input : List.of(new String[]{"node.crt", "rogue-pkcs1.key"}, new String[]{"ec-node.crt", "other-ec-sec1.key"}, new String[]{"ec-node.crt", "node-pkcs1.key"})) {
            Settings base = settings("mismatch", input[0], "ca.crt");
            Settings configured = Settings.builder().put(base).put("serverless.peer_tls.private_key", fixtures.resolve(input[1]))
                .put("serverless.http_tls.certificate", fixtures.resolve(input[0]))
                .put("serverless.http_tls.private_key", fixtures.resolve(input[1])).build();
            expectThrows(SSLException.class, () -> new PeerTlsPlugin(configured));
            expectThrows(SSLException.class, () -> new HttpTlsPlugin(configured));
        }
    }

    private void rotateHandshake(InetSocketAddress address, String client, String clientTrust, boolean accepted) throws Exception {
        try (SSLSocket socket = (SSLSocket) clientContext(client, clientTrust).getSocketFactory().createSocket()) {
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
            socket.setSoTimeout(5000);
            socket.connect(address, 5000);
            var parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(parameters);
            if (accepted) socket.startHandshake();
            else expectThrows(SSLException.class, socket::startHandshake);
        }
    }

    public void testCAOverlapAndRemovalForFreshHandshakes() throws Exception {
        for (String[] stage : List.of(
            new String[]{"node.crt", "overlap.crt"},
            new String[]{"new-node.crt", "overlap.crt"},
            new String[]{"new-node.crt", "other-ca.crt"})) {
            Settings ownerSettings = settings("owner", stage[0], stage[1]);
            try (ServerlessNode owner = new ServerlessNode(ownerSettings, List.of(new PeerTlsPlugin(ownerSettings)))) {
                owner.startup().start();
                InetSocketAddress address = owner.localNode().getAddress().address();
                boolean finalTrust = stage[1].equals("other-ca.crt");
                rotateHandshake(address, "node", "overlap.crt", finalTrust == false);
                rotateHandshake(address, "new-node", "overlap.crt", true);
                if (finalTrust) rotateHandshake(address, "new-node", "ca.crt", false);
            }
        }
    }

    public void testHttpTlsRequiresTrustedClientWhenConfigured() throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        Settings base = settings("https-node", "node.crt", "ca.crt");
        Settings configured = Settings.builder().put(base)
            .put("serverless.http_tls.certificate", fixtures.resolve("node.crt"))
            .put("serverless.http_tls.private_key", fixtures.resolve("node.key"))
            .put("serverless.http_tls.client_trust", fixtures.resolve("ca.crt"))
            .put("serverless.http_tls.require_client_certificate", true).build();
        try (ServerlessNode node = new ServerlessNode(configured, List.of(new PeerTlsPlugin(configured), new HttpTlsPlugin(configured)))) {
            node.startup().start();
            InetSocketAddress address = node.boundHttpAddress().publishAddress().address();
            rotateHandshake(address, null, "ca.crt", false);
            rotateHandshake(address, "rogue", "ca.crt", false);
            try (SSLSocket socket = (SSLSocket) clientContext("node", "ca.crt").getSocketFactory().createSocket()) {
                socket.setSoTimeout(5000);
                socket.connect(address, 5000);
                var parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                socket.getOutputStream().write("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                String status = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII)).readLine();
                assertNotNull(status);
                assertTrue(status.startsWith("HTTP/1.1 "));
            }
        }
    }

    public void testHttpTlsWithoutClientCertificateStillVerifiesServerIdentity() throws Exception {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        Settings base = settings("https-node", "node.crt", "ca.crt");
        Settings configured = Settings.builder().put(base)
            .put("serverless.http_tls.certificate", fixtures.resolve("node.crt"))
            .put("serverless.http_tls.private_key", fixtures.resolve("node.key")).build();
        try (ServerlessNode node = new ServerlessNode(configured, List.of(new PeerTlsPlugin(configured), new HttpTlsPlugin(configured)))) {
            node.startup().start();
            InetSocketAddress address = node.boundHttpAddress().publishAddress().address();
            rotateHandshake(address, null, "ca.crt", true);
            try (Socket raw = new Socket()) {
                raw.connect(address, 5000);
                try (SSLSocket socket = (SSLSocket) clientContext(null, "ca.crt").getSocketFactory()
                    .createSocket(raw, "wrong.invalid", address.getPort(), true)) {
                    socket.setSoTimeout(5000);
                    var parameters = socket.getSSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    socket.setSSLParameters(parameters);
                    expectThrows(SSLException.class, socket::startHandshake);
                }
            }
        }
    }
}
