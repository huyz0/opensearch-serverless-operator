/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.serverless.node;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.operator.PeerTlsPlugin;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PeerDnsIntegrationTests extends OpenSearchTestCase {
    private Settings settings(String name, String host) {
        Path fixtures = Path.of(System.getProperty("operator.tlsFixtures"));
        return Settings.builder().put("node.name", name).put("cluster.name", "peer-dns")
            .put("path.home", createTempDir()).put("network.host", "127.0.0.1").put("http.port", "0").put("transport.port", "0")
            .put("serverless.transport.advertise_host", host).put("transport.ssl.resolve_hostname", false)
            .put("serverless.peer_tls.certificate", fixtures.resolve("dns-node.crt"))
            .put("serverless.peer_tls.private_key", fixtures.resolve("node.key"))
            .put("serverless.peer_tls.trust", fixtures.resolve("ca.crt")).build();
    }

    public void testDnsLeaseAndVerifiedForwarding() throws Exception {
        Settings first = settings("owner", "localhost");
        Settings second = settings("peer", "localhost");
        AtomicLong clock = new AtomicLong(1000);
        MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30000);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        try (ServerlessNode owner = new ServerlessNode(first, List.of(new PeerTlsPlugin(first)));
             ServerlessNode peer = new ServerlessNode(second, List.of(new PeerTlsPlugin(second)))) {
            owner.startup().start();
            peer.startup().start();
            owner.setMetadataPlane(plane);
            peer.setMetadataPlane(plane);
            ((NodeContext) owner).nodeLogs.writeOwnLease(plane, List.of());
            ((NodeContext) peer).nodeLogs.writeOwnLease(plane, List.of());
            assertTrue(plane.membership().read(owner.localNode().getId()).orElseThrow().address().startsWith("localhost:"));
            owner.activation().activateWriter(plane, "alpha", 0);
            peer.activation().syncFrom(plane);
            assertTrue(peer.reconciler().openShards().isEmpty());
            peer.services().client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"dns\"}", XContentType.JSON)).actionGet();
            assertTrue(peer.services().client().get(new GetRequest("alpha", "k")).actionGet().isExists());
        }
    }

    public void testInvalidAdvertisedHostRejected() {
        for (String invalid : List.of("bad host", "node:9300", "bad..name", "-bad", "bad/route")) {
            Settings configured = settings("bad", invalid);
            expectThrows(IllegalArgumentException.class, () -> NodeLogStart.advertiseHost(configured));
        }
        assertEquals("", NodeLogStart.advertiseHost(Settings.EMPTY));
    }
}
