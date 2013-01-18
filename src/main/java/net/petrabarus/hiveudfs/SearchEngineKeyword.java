package net.petrabarus.hiveudfs;

import net.petrabarus.hiveudfs.helpers.InetAddrHelper;
import net.petrabarus.hiveudfs.helpers.KeywordParser;
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
 * This is a UDF to get keyword part from a search engine referrer URL.
 *
 * The function will need one argument which is the URL. If the URL is not
 * recognized as a search engine URL it will return null.
 *
 * Currently it only support simple URL from Google, Yahoo, and Bing. More to
 * come.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
@UDFType(deterministic = true)
@Description(
        name = "SearchEngineKeyword",
value = "_FUNC_(iplong) - returns keyword part from a search engine referrer URL",
extended = "Example:\n"
+ " > SELECT _FUNC_(\"http://www.google.com/search?q=keyword+keyword\") FROM table"
+ " > keyword keyword")
public class SearchEngineKeyword extends GenericUDF {

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
        public Object evaluate(DeferredObject[] arguments) throws HiveException {
                assert (arguments.length == 1);
                if (arguments[0].get() == null) {
                        return null;
                }
                String referrer = ((Text) converter.convert(arguments[0].get())).toString();
                KeywordParser kp = new KeywordParser(referrer);
                if (!kp.hasKeyword) {
                        return null;
                } else {
                        return new Text(kp.getKeyword());
                }
        }

        /**
         * Get the String to be displayed in explain.
         *
         * @return The display string.
         */
        @Override
        public String getDisplayString(String[] strings) {
                assert (strings.length == 1);
                return "_FUNC_(" + strings[0] + ")";
        }
}
