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
package net.petrabarus.hiveudfs;

import net.petrabarus.hiveudfs.helpers.InetAddrHelper;
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
 * IPToLong is a basic UDF to translate IP in string format to long format.
 *
 * Usage:
 * <pre>
 *      SELECT IPToLong(ipstring) FROM table;
 * </pre>
 *
 * @author Petra Barus <petra.barus@gmail.com>
 * @see http://muhmahmed.blogspot.com/2009/02/java-ip-address-to-long.html
 */
@UDFType(deterministic = true)
@Description(
        name = "IPToLong",
value = "_FUNC_(ipstring) - returns IP address in long format from string format.",
extended = "Example:\n"
+ " > SELECT _FUNC_(\"1.1.1.1\") FROM table"
+ " > 16843009")
public class IPToLong extends GenericUDF {

        private ObjectInspectorConverters.Converter converter;

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
                if (arguments.length != 1) {
                        throw new UDFArgumentLengthException("_FUNC_ expects only 1 argument.");
                }
                ObjectInspector argument = arguments[0];
                if (argument.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                        throw new UDFArgumentTypeException(0,
                                "A string argument was expected but an argument of type " + argument.getTypeName()
                                + " was given.");
                }
                PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) argument)
                        .getPrimitiveCategory();

                if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                        throw new UDFArgumentTypeException(0,
                                "A string argument was expected but an argument of type " + argument.getTypeName()
                                + " was given.");
                }
                converter = ObjectInspectorConverters.getConverter(argument, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
                assert (arguments.length == 1);
                if (arguments[0].get() == null) {
                        return null;
                }
                Text t = (Text) converter.convert(arguments[0].get());
                return new LongWritable(InetAddrHelper.IPToLong(t.toString()));
        }

        /**
         * Get the String to be displayed in explain.
         *
         * @return The display string.
         */
        @Override
        public String getDisplayString(String[] strings) {
                assert (strings.length == 3);
                return "translate(" + strings[0] + ")";
        }
}
