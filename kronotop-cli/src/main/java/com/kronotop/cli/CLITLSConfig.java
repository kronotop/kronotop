/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.cli;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles TLS socket creation for kronotop-cli.
 */
class CLITLSConfig {

    private static final Map<String, String> OPENSSL_TO_JSSE = Map.ofEntries(
            // TLSv1.2 ECDHE-RSA
            Map.entry("ECDHE-RSA-AES128-GCM-SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
            Map.entry("ECDHE-RSA-AES256-GCM-SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"),
            Map.entry("ECDHE-RSA-AES128-SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"),
            Map.entry("ECDHE-RSA-AES256-SHA384", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384"),
            Map.entry("ECDHE-RSA-AES128-SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"),
            Map.entry("ECDHE-RSA-AES256-SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"),
            Map.entry("ECDHE-RSA-CHACHA20-POLY1305", "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"),
            // TLSv1.2 ECDHE-ECDSA
            Map.entry("ECDHE-ECDSA-AES128-GCM-SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"),
            Map.entry("ECDHE-ECDSA-AES256-GCM-SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"),
            Map.entry("ECDHE-ECDSA-AES128-SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"),
            Map.entry("ECDHE-ECDSA-AES256-SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"),
            Map.entry("ECDHE-ECDSA-AES128-SHA", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"),
            Map.entry("ECDHE-ECDSA-AES256-SHA", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"),
            Map.entry("ECDHE-ECDSA-CHACHA20-POLY1305", "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"),
            // TLSv1.2 DHE-RSA
            Map.entry("DHE-RSA-AES128-GCM-SHA256", "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256"),
            Map.entry("DHE-RSA-AES256-GCM-SHA384", "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"),
            Map.entry("DHE-RSA-AES128-SHA256", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"),
            Map.entry("DHE-RSA-AES256-SHA256", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256"),
            Map.entry("DHE-RSA-AES128-SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA"),
            Map.entry("DHE-RSA-AES256-SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA"),
            Map.entry("DHE-RSA-CHACHA20-POLY1305", "TLS_DHE_RSA_WITH_CHACHA20_POLY1305_SHA256"),
            // RSA (no key exchange prefix in OpenSSL)
            Map.entry("AES128-GCM-SHA256", "TLS_RSA_WITH_AES_128_GCM_SHA256"),
            Map.entry("AES256-GCM-SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384"),
            Map.entry("AES128-SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA256"),
            Map.entry("AES256-SHA256", "TLS_RSA_WITH_AES_256_CBC_SHA256"),
            Map.entry("AES128-SHA", "TLS_RSA_WITH_AES_128_CBC_SHA"),
            Map.entry("AES256-SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
    );

    private static final TrustManager[] INSECURE_TRUST_MANAGERS = new TrustManager[]{
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }
    };

    private final boolean insecure;
    private final String sni;
    private final File cacert;
    private final File cacertdir;
    private final String tlsCiphers;
    private final String tlsCiphersuites;

    CLITLSConfig(boolean insecure, String sni, File cacert, File cacertdir,
                 String tlsCiphers, String tlsCiphersuites) {
        this.insecure = insecure;
        this.sni = sni;
        this.cacert = cacert;
        this.cacertdir = cacertdir;
        this.tlsCiphers = tlsCiphers;
        this.tlsCiphersuites = tlsCiphersuites;
    }

    private static String toJsseName(String cipher) {
        if (cipher.startsWith("TLS_") || cipher.startsWith("SSL_")) {
            return cipher;
        }
        return OPENSSL_TO_JSSE.getOrDefault(cipher, cipher);
    }

    /**
     * Wraps a connected plain socket with TLS and performs the handshake.
     */
    Socket wrapSocket(Socket plainSocket, String host, int port) throws IOException, GeneralSecurityException {
        SSLContext sslContext = SSLContext.getInstance("TLS");

        if (insecure) {
            sslContext.init(null, INSECURE_TRUST_MANAGERS, null);
        } else {
            TrustManager[] trustManagers = buildTrustManagers();
            sslContext.init(null, trustManagers, null);
        }

        SSLSocketFactory factory = sslContext.getSocketFactory();
        String sniHost = (sni != null) ? sni : host;
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(plainSocket, sniHost, port, true);

        // Set SNI and hostname verification
        SSLParameters params = sslSocket.getSSLParameters();
        params.setServerNames(List.of(new SNIHostName(sniHost)));
        if (!insecure) {
            params.setEndpointIdentificationAlgorithm("HTTPS");
        }
        sslSocket.setSSLParameters(params);

        // Set cipher suites
        List<String> ciphers = collectCiphers();
        if (!ciphers.isEmpty()) {
            sslSocket.setEnabledCipherSuites(ciphers.toArray(new String[0]));
        }

        sslSocket.startHandshake();
        return sslSocket;
    }

    private TrustManager[] buildTrustManagers() throws GeneralSecurityException, IOException {
        if (cacert == null && cacertdir == null) {
            // Use JDK default trust store
            return null;
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        int index = 0;

        if (cacert != null) {
            index = loadCertFile(cf, trustStore, cacert, index);
        }

        if (cacertdir != null) {
            if (!cacertdir.isDirectory()) {
                throw new IOException("--cacertdir path is not a directory: " + cacertdir);
            }
            File[] files = cacertdir.listFiles((dir, name) -> {
                String lower = name.toLowerCase();
                return lower.endsWith(".pem") || lower.endsWith(".crt") || lower.endsWith(".cer");
            });
            if (files != null) {
                for (File file : files) {
                    try {
                        index = loadCertFile(cf, trustStore, file, index);
                    } catch (GeneralSecurityException | IOException e) {
                        // Skip non-certificate files (e.g., private keys)
                    }
                }
            }
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf.getTrustManagers();
    }

    private int loadCertFile(CertificateFactory cf, KeyStore trustStore, File file, int index)
            throws GeneralSecurityException, IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            for (var cert : cf.generateCertificates(fis)) {
                trustStore.setCertificateEntry("cert-" + index, cert);
                index++;
            }
        }
        return index;
    }

    private List<String> collectCiphers() {
        List<String> result = new ArrayList<>();
        if (tlsCiphers != null && !tlsCiphers.isEmpty()) {
            for (String c : tlsCiphers.split(":")) {
                String trimmed = c.trim();
                if (!trimmed.isEmpty()) {
                    result.add(toJsseName(trimmed));
                }
            }
        }
        if (tlsCiphersuites != null && !tlsCiphersuites.isEmpty()) {
            for (String c : tlsCiphersuites.split(":")) {
                String trimmed = c.trim();
                if (!trimmed.isEmpty()) {
                    result.add(toJsseName(trimmed));
                }
            }
        }
        return result;
    }
}
