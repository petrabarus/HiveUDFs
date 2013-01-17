/**
 * GeoIP.java.
 *
 * Copyright (C) 2013 Petra Barus,
 *
 * Copyright (C) 2012 edwardcapriolo
 * https://github.com/edwardcapriolo/hive-geoip. .
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
package net.petrabarus.hiveudfs;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.RegionName;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * This is a UDF to look a property of an IP address using MaxMind GeoIP
 * library.
 *
 * The function will need three arguments. <ol> <li>IP Address in long
 * format.</li> <li>IP attribute (e.g. COUNTRY, CITY, REGION, etc)</li> <li>Database file name.</li> </ol>
 *
 * This is a derived version from https://github.com/edwardcapriolo/hive-geoip.
 * (Please let me know if I need to modify the license)
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @see https://github.com/edwardcapriolo/hive-geoip
 */
@UDFType(deterministic = true)
@Description(
  name = "geoip",
value = "_FUNC_(ip,property,database) - looks a property for an IP address from"
+ "a library loaded\n"
+ "The GeoIP database comes separated. To load the GeoIP use ADD FILE.\n"
+ "Usage:\n"
+ " > _FUNC_(16843009, \"COUNTRY_NAME\", \"./GeoIP.dat\")")
public class GeoIP extends GenericUDF {

        public static final String COUNTRY_NAME = "COUNTRY_NAME";
        public static final String COUNTRY_CODE = "COUNTRY_CODE";
        public static final String AREA_CODE = "AREA_CODE";
        public static final String CITY = "CITY";
        public static final String DMA_CODE = "DMA_CODE";
        public static final String LATITUDE = "LATITUDE";
        public static final String LONGITUDE = "LONGITUDE";
        public static final String METRO_CODE = "METRO_CODE";
        public static final String POSTAL_CODE = "POSTAL_CODE";
        public static final String REGION = "REGION";
        public static final String REGION_NAME = "REGION_NAME";
        public static final String ORG = "ORG";
        public static final String ID = "ID";
        private ObjectInspectorConverters.Converter[] converters;
        private static HashMap<String, LookupService> databases = new HashMap<String, LookupService>();

        /**
         * Initialize this UDF.
         *
         * This will be called once and only once per GenericUDF instance.
         *
         * @param arguments The ObjectInspector for the arguments
         * @throws UDFArgumentException Thrown when arguments have wrong types,
         * wrong length, etc.
         * @return The ObjectInspector for the return value
         */
        @Override
        public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
                if (arguments.length != 3) {
                        throw new UDFArgumentLengthException("_FUNC_ accepts 3 arguments. " + arguments.length
                                + " found.");
                }
                for (int i = 0; i < arguments.length; i++) {
                        if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                                throw new UDFArgumentTypeException(i,
                                        "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                                        + " was given.");
                        }
                }
                //first argument can be long or string
                PrimitiveObjectInspector.PrimitiveCategory firstParamPrimitiveCategory = ((PrimitiveObjectInspector) arguments[0])
                        .getPrimitiveCategory();
                if (firstParamPrimitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
                        throw new UDFArgumentTypeException(0,
                                "A string or long for first argument was expected but an argument of type " + arguments[0].getTypeName()
                                + " was given.");
                }

                for (int i = 1; i < arguments.length; i++) {
                        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) arguments[i])
                                .getPrimitiveCategory();
                        if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.STRING
                                && primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.VOID) {
                                throw new UDFArgumentTypeException(i,
                                        "A string argument was expected but an argument of type " + arguments[i].getTypeName()
                                        + " was given.");
                        }
                }

                converters = new ObjectInspectorConverters.Converter[arguments.length];
                converters[0] = ObjectInspectorConverters.getConverter(arguments[0],
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                for (int i = 1; i < arguments.length; i++) {
                        converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                }
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }

        /**
         * Evaluate the UDF with the arguments.
         *
         * @param arguments The arguments as DeferedObject, use
         * DeferedObject.get() to get the actual argument Object. The Objects
         * can be inspected by the ObjectInspectors passed in the initialize
         * call.
         * @return The return value.
         */
        @Override
        public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
                assert (arguments.length == 3);
                LongWritable ipArg = (LongWritable) converters[0].convert(arguments[0].get());
                long ip = ipArg.get();
                String attributeName = ((Text) converters[1].convert(arguments[1].get())).toString();
                String databaseName = ((Text) converters[2].convert(arguments[2].get())).toString();
                LookupService lookupService;
                //Just in case there are more than one database filename attached.
                //We will just assume that two file with same filename are identical.
                if (!databases.containsKey(databaseName)) {
                        File file = new File(databaseName);
                        if (!file.exists()) {
                                throw new HiveException(databaseName + " does not exist");
                        }
                        try {
                                lookupService = new LookupService(file, LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
                                databases.put(databaseName, lookupService);
                        } catch (IOException ex) {
                                throw new HiveException(ex);
                        }
                } else {
                        lookupService = databases.get(databaseName);
                }
                String retVal = "";
                try {
                        //Let's do it baby!
                        Location location = lookupService.getLocation(ip);
                        if (attributeName.equals(COUNTRY_NAME)) {
                                retVal = location.countryName;
                        } else if (attributeName.equals(COUNTRY_CODE)) {
                                retVal = location.countryCode;
                        } else if (attributeName.equals(AREA_CODE)) {
                                retVal = location.area_code + "";
                        } else if (attributeName.equals(CITY)) {
                                retVal = location.city + "";
                        } else if (attributeName.equals(DMA_CODE)) {
                                retVal = location.dma_code + "";
                        } else if (attributeName.equals(LATITUDE)) {
                                retVal = location.latitude + "";
                        } else if (attributeName.equals(LONGITUDE)) {
                                retVal = location.longitude + "";
                        } else if (attributeName.equals(METRO_CODE)) {
                                retVal = location.metro_code + "";
                        } else if (attributeName.equals(POSTAL_CODE)) {
                                retVal = location.postalCode;
                        } else if (attributeName.equals(REGION)) {
                                retVal = location.region + "";
                        } else if (attributeName.equals(REGION_NAME)) {
                                retVal = RegionName.regionNameByCode(location.countryCode, location.region);
                        } else if (attributeName.equals(ORG)) {
                                retVal = lookupService.getOrg(ip);
                        } else if (attributeName.equals(ID)) {
                                retVal = lookupService.getID(ip) + "";
                        }
                } catch (Exception ex) {
                        //This will be useful if you don't have a complete database file.
                        return null;
                }
                return new Text(retVal);
        }

        /**
         * Get the String to be displayed in explain.
         *
         * @return The display string.
         */
        @Override
        public String getDisplayString(String[] children) {
                assert (children.length == 3);
                return "_FUNC_( " + children[0] + ", " + children[1] + ", " + children[2] + " )";
        }
}
