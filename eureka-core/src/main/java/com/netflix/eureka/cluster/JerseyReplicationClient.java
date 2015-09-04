package com.netflix.eureka.cluster;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaIdentityHeaderFilter;
import com.netflix.discovery.shared.EurekaJerseyClient;
import com.netflix.discovery.shared.EurekaJerseyClient.JerseyClient;
import com.netflix.discovery.shared.JerseyEurekaHttpClient;
import com.netflix.eureka.EurekaServerConfig;
import com.netflix.eureka.EurekaServerIdentity;
import com.netflix.eureka.cluster.protocol.ReplicationList;
import com.netflix.eureka.cluster.protocol.ReplicationListResponse;
import com.netflix.eureka.resources.ASGResource.ASGStatus;

/**
 * @author Tomasz Bak
 */
public class JerseyReplicationClient extends JerseyEurekaHttpClient implements HttpReplicationClient {

    private static final Logger logger = LoggerFactory.getLogger(JerseyReplicationClient.class);

    private final JerseyClient jerseyClient;
    private final Client jerseyApacheClient;

    public JerseyReplicationClient(EurekaServerConfig config, String serviceUrl) {
        super(serviceUrl);
        String name = getClass().getSimpleName() + ": " + serviceUrl + "apps/: ";

        try {
            String hostname;
            try {
                hostname = new URL(serviceUrl).getHost();
            } catch (MalformedURLException e) {
                hostname = serviceUrl;
            }
            String jerseyClientName = "Discovery-PeerNodeClient-" + hostname;
            if (serviceUrl.startsWith("https://") &&
                    "true".equals(System.getProperty("com.netflix.eureka.shouldSSLConnectionsUseSystemSocketFactory"))) {
                jerseyClient = EurekaJerseyClient.createSystemSSLJerseyClient(jerseyClientName,
                        config.getPeerNodeConnectTimeoutMs(),
                        config.getPeerNodeReadTimeoutMs(),
                        config.getPeerNodeTotalConnections(),
                        config.getPeerNodeTotalConnectionsPerHost(),
                        config.getPeerNodeConnectionIdleTimeoutSeconds());
            } else {
                jerseyClient = EurekaJerseyClient.createJerseyClient(jerseyClientName,
                        config.getPeerNodeConnectTimeoutMs(),
                        config.getPeerNodeReadTimeoutMs(),
                        config.getPeerNodeTotalConnections(),
                        config.getPeerNodeTotalConnectionsPerHost(),
                        config.getPeerNodeConnectionIdleTimeoutSeconds());
            }
            jerseyApacheClient = jerseyClient.getClient();
            jerseyApacheClient.register(GZipEncoder.class);
            jerseyApacheClient.register(EncodingFilter.class);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot Create new Replica Node :" + name, e);
        }

        String ip = null;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Cannot find localhost ip", e);
        }
        EurekaServerIdentity identity = new EurekaServerIdentity(ip);
        jerseyApacheClient.register(new EurekaIdentityHeaderFilter(identity));
    }

    @Override
    protected Client getJerseyApacheClient() {
        return jerseyApacheClient;
    }

    @Override
    protected void addExtraHeaders(Builder webResource) {
        webResource.header(PeerEurekaNode.HEADER_REPLICATION, "true");
    }

    /**
     * Compared to regular heartbeat, in the replication channel the server may return a more up to date
     * instance copy.
     */
    @Override
    public HttpResponse<InstanceInfo> sendHeartBeat(String appName, String id, InstanceInfo info, InstanceStatus overriddenStatus) {
        String urlPath = "apps/" + appName + '/' + id;
        Response response = null;
        try {
            WebTarget webResource = getJerseyApacheClient().target(serviceUrl)
                    .path(urlPath)
                    .queryParam("status", info.getStatus().toString())
                    .queryParam("lastDirtyTimestamp", info.getLastDirtyTimestamp().toString());
            if (overriddenStatus != null) {
                webResource = webResource.queryParam("overriddenstatus", overriddenStatus.name());
            }
            Builder requestBuilder = webResource.request();
            addExtraHeaders(requestBuilder);
            response = requestBuilder.accept(MediaType.APPLICATION_JSON_TYPE).put(null);
            InstanceInfo infoFromPeer = null;
            if (response.getStatus() == Status.CONFLICT.getStatusCode() && response.hasEntity()) {
                infoFromPeer = response.readEntity(InstanceInfo.class);
            }
            return HttpResponse.responseWith(response.getStatus(), infoFromPeer);
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("[heartbeat] Jersey HTTP PUT {}; statusCode={}", urlPath, response == null ? "N/A" : response.getStatus());
            }
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public HttpResponse<Void> statusUpdate(String asgName, ASGStatus newStatus) {
        Response response = null;
        try {
            String urlPath = "asg/" + asgName + "/status";
            response = jerseyApacheClient.target(serviceUrl)
                    .path(urlPath)
                    .queryParam("value", newStatus.name())
                    .request()
                    .header(PeerEurekaNode.HEADER_REPLICATION, "true")
                    .put(null);
            return HttpResponse.responseWith(response.getStatus());
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public HttpResponse<ReplicationListResponse> submitBatchUpdates(ReplicationList replicationList) {
        Response response = null;
        try {
            response = jerseyApacheClient.target(serviceUrl)
                    .path(PeerEurekaNode.BATCH_URL_PATH)
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(replicationList));
            if (!isSuccess(response.getStatus())) {
                return HttpResponse.responseWith(response.getStatus());
            }
            ReplicationListResponse batchResponse = response.readEntity(ReplicationListResponse.class);
            return HttpResponse.responseWith(response.getStatus(), batchResponse);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        jerseyClient.destroyResources();
    }

    private static boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
