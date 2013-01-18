/**
 * Copyright (C) 2013 Petra Barus.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package net.petrabarus.hiveudfs.helpers;

/**
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

/**
 * QueryParams handles query string map from a URL.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class QueryParams {

        private static class KVP {

                final String key;
                final String value;

                KVP(String key, String value) {
                        this.key = key;
                        this.value = value;
                }
        }
        List<KVP> query = new ArrayList<KVP>();

        /**
         * Construct query sets and parse a query string right away.
         *
         * @param queryString the query string.
         */
        public QueryParams(String queryString) {
                parse(queryString);
        }

        public QueryParams() {
        }

        /**
         * Add a param to the query string map.
         *
         * @param key query key.
         * @param value query value.
         */
        public void addParam(String key, String value) {
                if (key == null || value == null) {
                        throw new NullPointerException("null parameter key or value");
                }
                query.add(new KVP(key, value));
        }

        /**
         * Parse a query string.
         *
         * @param queryString the query string.
         */
        private void parse(String queryString) {
                for (String pair : queryString.split("&")) {
                        int eq = pair.indexOf("=");
                        if (eq < 0) {
                                try {
                                        addParam(URLDecoder.decode(pair, "UTF-8"), "");
                                } catch (UnsupportedEncodingException ex) {
                                }
                        } else {
                                try {
                                        String key = URLDecoder.decode(pair.substring(0, eq), "UTF-8");
                                        String value = URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
                                        query.add(new KVP(key, value));
                                } catch (UnsupportedEncodingException ex) {
                                }
                        }
                }
        }

        /**
         * Return query map in URL query string format.
         *
         * @return the query in string format.
         */
        public String toQueryString() {
                StringBuilder sb = new StringBuilder();
                for (KVP kvp : query) {
                        if (sb.length() > 0) {
                                sb.append('&');
                        }
                        try {
                                sb.append(URLEncoder.encode(kvp.key, "UTF-8"));
                        } catch (UnsupportedEncodingException ex) {
                        }
                        if (!kvp.value.equals("")) {
                                sb.append('=');
                                try {
                                        sb.append(URLEncoder.encode(kvp.value, "UTF-8"));
                                } catch (UnsupportedEncodingException ex) {
                                }
                        }
                }
                return sb.toString();
        }

        /**
         * Get parameter from query map.
         *
         * @param key query key.
         * @return the parameter..
         */
        public String getParameter(String key) {
                for (KVP kvp : query) {
                        if (kvp.key.equals(key)) {
                                return kvp.value;
                        }
                }
                return null;
        }

        /**
         * Get list of values from a parameter.
         *
         * @param key the query key.
         * @return list of values.
         */
        public List<String> getParameterValues(String key) {
                List<String> list = new LinkedList<String>();
                for (KVP kvp : query) {
                        if (kvp.key.equals(key)) {
                                list.add(kvp.value);
                        }
                }
                return list;
        }
}