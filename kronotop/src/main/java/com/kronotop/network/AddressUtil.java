/*
 * Copyright (c) 2023-2024 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.network;

import java.net.*;
import java.util.Enumeration;

public class AddressUtil {
    public static InetAddress getIPv4Address() {
        InetAddress host;
        try {
            host = InetAddress.getLocalHost();
            if (host instanceof Inet4Address && !host.isLinkLocalAddress() && !host.isLoopbackAddress()) {
                return host;
            }
        } catch (UnknownHostException e) {
            String s = "Local host not found";
            throw new RuntimeException(s, e);
        }
        try {
            Enumeration<NetworkInterface> i = NetworkInterface.getNetworkInterfaces();
            while (i.hasMoreElements()) {
                NetworkInterface ni = i.nextElement();
                Enumeration<InetAddress> j = ni.getInetAddresses();
                while (j.hasMoreElements()) {
                    InetAddress inetAddress = j.nextElement();
                    if (!inetAddress.isLinkLocalAddress() && !inetAddress.isLoopbackAddress()
                            && (inetAddress instanceof Inet4Address)) {
                        return inetAddress;
                    }
                }
            }
            String s = "IPv4 address not found";
            throw new RuntimeException(s);
        } catch (SocketException e) {
            String s = "Problem reading IPv4 address";
            throw new RuntimeException(s, e);
        }
    }
}