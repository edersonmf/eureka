package com.netflix.appinfo.providers;

import javax.inject.Inject;

import java.util.Map;

import com.google.common.base.Strings;
import com.google.inject.Provider;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.InstanceInfo.PortType;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.governator.guice.lazy.LazySingleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InstanceInfo provider that constructs the InstanceInfo this this instance using
 * EurekaInstanceConfig.
 *
 * This provider is @Singleton scope as it provides the InstanceInfo for both DiscoveryClient
 * and ApplicationInfoManager, and need to provide the same InstanceInfo to both.
 *
 * @author elandau
 *
 */
@LazySingleton
public class EurekaConfigBasedInstanceInfoProvider implements Provider<InstanceInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(EurekaConfigBasedInstanceInfoProvider.class);

    private final EurekaInstanceConfig config;

    private InstanceInfo instanceInfo;

    @Inject
    public EurekaConfigBasedInstanceInfoProvider(EurekaInstanceConfig config) {
        this.config = config;
    }

    @Override
    public synchronized InstanceInfo get() {
        if (instanceInfo == null) {
            // Build the lease information to be passed to the server based
            // on config
            LeaseInfo.Builder leaseInfoBuilder = LeaseInfo.Builder
                    .newBuilder()
                    .setRenewalIntervalInSecs(
                            config.getLeaseRenewalIntervalInSeconds())
                    .setDurationInSecs(
                            config.getLeaseExpirationDurationInSeconds());

            // Builder the instance information to be registered with eureka
            // server
            InstanceInfo.Builder builder = InstanceInfo.Builder.newBuilder();

            builder.setNamespace(config.getNamespace())
                    .setAppName(config.getAppname())
                    .setAppGroupName(config.getAppGroupName())
                    .setDataCenterInfo(config.getDataCenterInfo())
                    .setIPAddr(IPResolver.resolve(config.getIpAddress()))
                    .setHostName(config.getHostName(false))
                    .setPort(config.getNonSecurePort())
                    .enablePort(PortType.UNSECURE,
                            config.isNonSecurePortEnabled())
                    .setSecurePort(config.getSecurePort())
                    .enablePort(PortType.SECURE, config.getSecurePortEnabled())
                    .setVIPAddress(config.getVirtualHostName())
                    .setSecureVIPAddress(config.getSecureVirtualHostName())
                    .setHomePageUrl(config.getHomePageUrlPath(),
                            config.getHomePageUrl())
                    .setStatusPageUrl(config.getStatusPageUrlPath(),
                            config.getStatusPageUrl())
                    .setHealthCheckUrls(config.getHealthCheckUrlPath(),
                            config.getHealthCheckUrl(),
                            config.getSecureHealthCheckUrl())
                    .setASGName(config.getASGName());

            // Start off with the STARTING state to avoid traffic
            if (!config.isInstanceEnabledOnit()) {
                InstanceStatus initialStatus = InstanceStatus.STARTING;
                LOG.info("Setting initial instance status as: " + initialStatus);
                builder.setStatus(initialStatus);
            } else {
                LOG.info("Setting initial instance status as: " + InstanceStatus.UP
                        + ". This may be too early for the instance to advertise itself as available. "
                        + "You would instead want to control this via a healthcheck handler.");
            }

            // Add any user-specific metadata information
            for (Map.Entry<String, String> mapEntry : config.getMetadataMap()
                    .entrySet()) {
                String key = mapEntry.getKey();
                String value = mapEntry.getValue();
                builder.add(key, value);
            }

            instanceInfo = builder.build();
            instanceInfo.setLeaseInfo(leaseInfoBuilder.build());
        }
        return instanceInfo;
    }

    /**
     * IP address resolver.
     * Typically this is useful when running eureka client inside linux containers. Containers always register
     * themselves with local container's ip address, and in most cases it is supposed to be registered with
     * host's IP address.
     *
     * @author Ederson Ferreira <a href="mailto:edersonmf@gmail.com">edersonmf@gmail.com</a>
     */
    private static final class IPResolver {

        private static final String SYSTEM_PROP_IP = System.getProperty("eureka.instance.ip-address");

        private static final String ENV_IP = System.getenv("EUREKA_INSTANCE_IP");

        /**
         * Resolves the IP address to something provided through system properties ({@value #SYSTEM_PROP_IP}
         * or environment variable {@value #ENV_IP}.
         * It follows precedence as: System.getProperty(); System.getenv() or defaults to value passed as parameter.
         * @param defaultIp used as default IP address if no system property or environment variables are provided.
         * @return the IP address resolved.
         */
        public static final String resolve(final String defaultIp) {
            if (!Strings.isNullOrEmpty(SYSTEM_PROP_IP)) {
                return SYSTEM_PROP_IP;
            }
            if (!Strings.isNullOrEmpty(ENV_IP)) {
                return SYSTEM_PROP_IP;
            }
            return defaultIp;
        }

    }
}
