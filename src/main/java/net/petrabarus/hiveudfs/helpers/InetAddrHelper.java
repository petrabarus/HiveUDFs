package net.petrabarus.hiveudfs.helpers;

/**
 * This is a helper for IP address manipulation functions.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class InetAddrHelper {

        /**
         * Converts IP address to long format.
         *
         * @param addr the IP address in string format.
         * @return the IP address in long format.
         * @see
         * http://muhmahmed.blogspot.com/2009/02/java-ip-address-to-long.html
         */
        public static long IPToLong(String addr) {
                String[] addrArray = addr.split("\\.");
                long num = 0;
                for (int i = 0; i < addrArray.length; i++) {
                        int power = 3 - i;

                        num += ((Integer.parseInt(addrArray[i]) % 256 * Math.pow(256, power)));
                }
                return num;
        }

        /**
         * Converts long to IP address.
         *
         * @param ip the IP address in long.
         * @return the IP address in string.
         *
         * @see
         * http://muhmahmed.blogspot.com/2009/02/java-ip-address-to-long.html
         */
        public static String longToIP(long ip) {
                return ((ip >> 24) & 0xFF) + "."
                        + ((ip >> 16) & 0xFF) + "."
                        + ((ip >> 8) & 0xFF) + "."
                        + (ip & 0xFF);

        }
}
