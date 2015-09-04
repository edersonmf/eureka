/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.discovery.shared;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;

import com.google.common.base.Preconditions;
import com.netflix.discovery.provider.DiscoveryJerseyProvider;

/**
 * A wrapper for Jersey Apache Client to set the necessary configurations.
 *
 * @author Karthik Ranganathan
 *
 */
public final class EurekaJerseyClient {

    private EurekaJerseyClient() {
    }

    /**
     * Creates a Jersey client with the given configuration parameters.
     *
     *
     * @param clientName
     * @param connectionTimeout
     *            - The connection timeout of the connection in milliseconds
     * @param readTimeout
     *            - The read timeout of the connection in milliseconds
     * @param maxConnectionsPerHost
     *            - The maximum number of connections to a particular host
     * @param maxTotalConnections
     *            - The maximum number of total connections across all hosts
     * @param connectionIdleTimeout
     *            - The idle timeout after which the connections will be cleaned
     *            up in seconds
     * @return - The jersey client object encapsulating the connection
     */
    public static JerseyClient createJerseyClient(String clientName, int connectionTimeout,
                                                  int readTimeout, int maxConnectionsPerHost,
                                                  int maxTotalConnections, int connectionIdleTimeout) {
        Preconditions.checkNotNull(clientName, "Client name can not be null.");
        try {
            ClientConfig jerseyClientConfig = new CustomApacheHttpClientConfig(clientName, maxConnectionsPerHost,
                    maxTotalConnections, connectionTimeout, readTimeout, connectionIdleTimeout);

            return new JerseyClient(jerseyClientConfig);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot create Jersey client ", e);
        }
    }

    /**
     * Creates a Jersey client with the given configuration parameters.
     *
     *
     * @param clientName
     * @param connectionTimeout
     *            - The connection timeout of the connection in milliseconds
     * @param readTimeout
     *            - The read timeout of the connection in milliseconds
     * @param maxConnectionsPerHost
     *            - The maximum number of connections to a particular host
     * @param maxTotalConnections
     *            - The maximum number of total connections across all hosts
     * @param connectionIdleTimeout
     *            - The idle timeout after which the connections will be cleaned
     *            up in seconds
     * @param proxyHost
     *            - The hostname of the proxy
     * @param proxyPort
     *            - The port number the proxy is listening on
     * @param proxyUserName
     *            - The username to use to authenticate to the proxy
     * @param proxyPassword
     *            - The password to use to authenticate to the proxy
     * @return - The jersey client object encapsulating the connection
     */
    public static JerseyClient createProxyJerseyClient(String clientName, int connectionTimeout,
                                                       int readTimeout, int maxConnectionsPerHost, int maxTotalConnections, int connectionIdleTimeout,
                                                       String proxyHost, String proxyPort) {
        Preconditions.checkNotNull(clientName, "Client name can not be null.");
        try {
            ClientConfig jerseyClientConfig = new ProxyCustomApacheHttpClientConfig(clientName, maxConnectionsPerHost,
                    maxTotalConnections, proxyHost, proxyPort, connectionTimeout, readTimeout, connectionIdleTimeout);

            return new JerseyClient(jerseyClientConfig);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot create Jersey client ", e);
        }
    }

    /**
     * Creates the SSL based Jersey client with the given configuration
     * parameters.
     *
     *
     *
     * @param clientName
     * @param connectionTimeout
     *            - The connection timeout of the connection in milliseconds
     * @param readTimeout
     *            - The read timeout of the connection in milliseconds
     * @param maxConnectionsPerHost
     *            - The maximum number of connections to a particular host
     * @param maxTotalConnections
     *            - The maximum number of total connections across all hosts
     * @param connectionIdleTimeout
     *            - The idle timeout after which the connections will be cleaned
     *            up in seconds
     * @param trustStoreFileName
     *            - The full path to the trust store file
     * @param trustStorePassword
     *            - The password of the trust store file
     * @return - The jersey client object encapsulating the connection
     */

    public static JerseyClient createSSLJerseyClient(String clientName, int connectionTimeout,
                                                     int readTimeout, int maxConnectionsPerHost,
                                                     int maxTotalConnections, int connectionIdleTimeout,
                                                     String trustStoreFileName, String trustStorePassword) {
        Preconditions.checkNotNull(clientName, "Client name can not be null.");
        try {
            ClientConfig jerseyClientConfig = new SSLCustomApacheHttpClientConfig(
                    clientName, maxConnectionsPerHost, maxTotalConnections,
                    trustStoreFileName, trustStorePassword, connectionTimeout, readTimeout, connectionIdleTimeout);

            return new JerseyClient(jerseyClientConfig);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot create SSL Jersey client ", e);
        }
    }

    /**
     * Creates the SSL based Jersey client with the given configuration
     * parameters and using a SystemSocketFactory to support standard keystore/truststore 
     * system properties.
     *
     * @param clientName
     * @param connectionTimeout
     *            - The connection timeout of the connection in milliseconds
     * @param readTimeout
     *            - The read timeout of the connection in milliseconds
     * @param maxConnectionsPerHost
     *            - The maximum number of connections to a particular host
     * @param maxTotalConnections
     *            - The maximum number of total connections across all hosts
     * @param connectionIdleTimeout
     *            - The idle timeout after which the connections will be cleaned
     *            up in seconds
     * @return - The jersey client object encapsulating the connection
     */

    public static JerseyClient createSystemSSLJerseyClient(String clientName, int connectionTimeout,
                                                           int readTimeout, int maxConnectionsPerHost,
                                                           int maxTotalConnections, int connectionIdleTimeout) {
        Preconditions.checkNotNull(clientName, "Client name can not be null.");
        try {
            ClientConfig jerseyClientConfig = new SystemSSLCustomApacheHttpClientConfig(
                    clientName, maxConnectionsPerHost, maxTotalConnections, connectionTimeout, readTimeout, connectionIdleTimeout);

            return new JerseyClient(jerseyClientConfig);
        } catch (Throwable e) {
            throw new RuntimeException("Cannot create System SSL Jersey client ", e);
        }
    }

    private static class CustomApacheHttpClientConfig extends ClientConfig {

        public CustomApacheHttpClientConfig(String clientName, int maxConnectionsPerHost, int maxTotalConnections, int connectionTimeout, int readTimeout, int connectionIdleTimeout)
                throws Throwable {

            connectorProvider(new ApacheConnectorProvider());
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setDefaultMaxPerRoute(maxConnectionsPerHost);
            cm.setMaxTotal(maxTotalConnections);
            cm.closeExpiredConnections();
            cm.closeIdleConnections(connectionIdleTimeout, TimeUnit.SECONDS);
            property(ApacheClientProperties.CONNECTION_MANAGER, cm);

            // To pin a client to specific server in case redirect happens, we handle redirects directly
            // (see DiscoveryClient.makeRemoteCall methods).
            RequestConfig requestConfig = RequestConfig.custom()
                .setRedirectsEnabled(Boolean.FALSE)
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(readTimeout)
                .build();
            property(ApacheClientProperties.REQUEST_CONFIG, requestConfig);
        }
    }

    private static class ProxyCustomApacheHttpClientConfig extends ClientConfig {

        public ProxyCustomApacheHttpClientConfig(String clientName, int maxConnectionsPerHost, int maxTotalConnections,
                                                 String proxyHost, String proxyPort, int connectionTimeout, int readTimeout, int connectionIdleTimeout) throws Throwable {
            connectorProvider(new ApacheConnectorProvider());
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setDefaultMaxPerRoute(maxConnectionsPerHost);
            cm.setMaxTotal(maxTotalConnections);
            cm.closeExpiredConnections();
            cm.closeIdleConnections(connectionIdleTimeout, TimeUnit.SECONDS);
            property(ApacheClientProperties.CONNECTION_MANAGER, cm);
            // To pin a client to specific server in case redirect happens, we handle redirects directly
            // (see DiscoveryClient.makeRemoteCall methods).
            RequestConfig requestConfig = RequestConfig.custom()
                                                       .setRedirectsEnabled(Boolean.FALSE)
                                                       .setProxy(new HttpHost(InetAddress.getByName(proxyHost), Integer.valueOf(proxyPort), "http"))
                                                       .setConnectTimeout(connectionTimeout)
                                                       .setSocketTimeout(readTimeout)
                                                       .build();
            property(ApacheClientProperties.REQUEST_CONFIG, requestConfig);
        }
    }

    private static class SSLCustomApacheHttpClientConfig extends ClientConfig {
        private static final String PROTOCOL_SCHEME = "SSL";
        private static final String PROTOCOL = "https";
        private static final String KEYSTORE_TYPE = "JKS";

        public SSLCustomApacheHttpClientConfig(String clientName, int maxConnectionsPerHost,
                                               int maxTotalConnections, String trustStoreFileName,
                                               String trustStorePassword, int connectionTimeout, int readTimeout, int connectionIdleTimeout) throws Throwable {

            SSLContext sslContext = SSLContext.getInstance(PROTOCOL_SCHEME);
            TrustManagerFactory tmf = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore sslKeyStore = KeyStore.getInstance(KEYSTORE_TYPE);
            FileInputStream fin = null;
            try {
                fin = new FileInputStream(trustStoreFileName);
                sslKeyStore.load(fin, trustStorePassword.toCharArray());
                tmf.init(sslKeyStore);
                sslContext.init(null, createTrustManagers(sslKeyStore), null);

                connectorProvider(new ApacheConnectorProvider());

                SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext);
                Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register(PROTOCOL, sslConnectionSocketFactory)
                    .build();

                PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
//                MonitoredConnectionManager cm = new MonitoredConnectionManager(clientName, sslSchemeRegistry);
                cm.setDefaultMaxPerRoute(maxConnectionsPerHost);
                cm.setMaxTotal(maxTotalConnections);
                cm.closeExpiredConnections();
                cm.closeIdleConnections(connectionIdleTimeout, TimeUnit.SECONDS);
                // To pin a client to specific server in case redirect happens, we handle redirects directly
                // (see DiscoveryClient.makeRemoteCall methods).
                property(ApacheClientProperties.CONNECTION_MANAGER, cm);
                RequestConfig requestConfig = RequestConfig.custom()
                    .setRedirectsEnabled(Boolean.FALSE)
                    .setConnectTimeout(connectionTimeout)
                    .setSocketTimeout(readTimeout)
                    .build();
                property(ApacheClientProperties.REQUEST_CONFIG, requestConfig);
            } finally {
                if (fin != null) {
                    fin.close();
                }
            }

        }

        private static TrustManager[] createTrustManagers(KeyStore trustStore) {
            TrustManagerFactory factory;
            try {
                factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                factory.init(trustStore);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

            final TrustManager[] managers = factory.getTrustManagers();

            return managers;

        }
    }

    private static class SystemSSLCustomApacheHttpClientConfig extends ClientConfig {
        private static final String PROTOCOL = "https";

        public SystemSSLCustomApacheHttpClientConfig(String clientName, int maxConnectionsPerHost,
                                                     int maxTotalConnections, int connectionTimeout, int readTimeout, int connectionIdleTimeout) throws Throwable {

            connectorProvider(new ApacheConnectorProvider());
  
            SSLConnectionSocketFactory sslConnectionSocketFactory = SSLConnectionSocketFactory.getSystemSocketFactory();
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register(PROTOCOL, sslConnectionSocketFactory)
                .build();
  
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
            cm.setDefaultMaxPerRoute(maxConnectionsPerHost);
            cm.setMaxTotal(maxTotalConnections);
            cm.closeExpiredConnections();
            cm.closeIdleConnections(connectionIdleTimeout, TimeUnit.SECONDS);
            property(ApacheClientProperties.CONNECTION_MANAGER, cm);
            // To pin a client to specific server in case redirect happens, we handle redirects directly
            // (see DiscoveryClient.makeRemoteCall methods).
            RequestConfig requestConfig = RequestConfig.custom()
                .setRedirectsEnabled(Boolean.FALSE)
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(readTimeout)
                .build();
            property(ApacheClientProperties.REQUEST_CONFIG, requestConfig);
        }
    }

    public static class JerseyClient {

        private Client jerseyClient;

        ClientConfig jerseyClientConfig;

        private ScheduledExecutorService eurekaConnCleaner =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "Eureka-JerseyClient-Conn-Cleaner" + threadNumber.incrementAndGet());
                        thread.setDaemon(true);
                        return thread;
                    }
                });

        public Client getClient() {
            return jerseyClient;
        }

        public ClientConfig getClientconfig() {
            return jerseyClientConfig;
        }

        public JerseyClient(ClientConfig clientConfig) {
            try {
                jerseyClientConfig = clientConfig;
                jerseyClientConfig.register(DiscoveryJerseyProvider.class);
                jerseyClient = ClientBuilder.newBuilder().withConfig(jerseyClientConfig).build();
            } catch (Throwable e) {
                throw new RuntimeException("Cannot create Jersey client", e);
            }

        }

        /**
         * Clean up resources.
         */
        public void destroyResources() {
            if (eurekaConnCleaner != null) {
                eurekaConnCleaner.shutdown();
            }
        }
    }

}
