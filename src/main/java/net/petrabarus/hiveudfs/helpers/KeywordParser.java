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

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Extract keyword URL for Google, Bing, and Yahoo.
 *
 * This is a very simple class to extract search engine keywords from a small
 * set of Google, Bing, and Yahoo referrer URL.
 *
 * Need to expand this to handle a large set of other search engines and custom
 * parsing on the URL path.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class KeywordParser {

        /**
         * The string version of the URL.
         */
        private String urlString;
        /**
         * The URL in URL format.
         *
         * @see URL.
         */
        private URL url;
        /**
         * The parsed keyword.
         */
        private String keyword = "";
        /**
         * Whether the URL has keyword or not.
         */
        public boolean hasKeyword;

        /**
         * Construct the parser and parse the keyword right away.
         *
         * @param urlString the URL.
         */
        public KeywordParser(String urlString) {
                this.urlString = urlString;
                this.hasKeyword = true;
                try {
                        this.url = new URL(urlString);
                } catch (MalformedURLException ex) {
                        this.hasKeyword = false;
                        return;
                }
                String queryString = url.getQuery();
                if (queryString == null || queryString.length() == 0) {
                        this.hasKeyword = false;
                        return;
                }
                QueryParams qp = new QueryParams(queryString);

                //TODO really need to use a much better design pattern for this crap.
                if (url.getHost().contains("google")) {
                        this.keyword = qp.getParameter("q");
                } else if (url.getHost().contains("bing.com")) {
                        this.keyword = qp.getParameter("q");
                } else if (url.getHost().contains("yahoo")) {
                        this.keyword = qp.getParameter("p");
                } else if (url.getHost().contains("ask.com")) {
                        this.keyword = qp.getParameter("q");
                } else {
                        this.hasKeyword = false;
                        return;
                }
                if (this.keyword == null || this.keyword.length() == 0) {
                        this.keyword = "";
                        this.hasKeyword = false;
                }

        }

        /**
         * Return the URL string.
         *
         * @return the url string.
         */
        public String getUrlString() {
                return urlString;
        }

        /**
         * Return the URL.
         *
         * @return URL.
         */
        public URL getUrl() {
                return url;
        }

        /**
         * Return the keyword parsed in the initial.
         *
         * @return the keyword.
         */
        public String getKeyword() {
                return keyword;
        }
}
