package net.petrabarus.hiveudfs;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * UCWords is equivalent to PHP's ucwords().
 *
 * It's just a wrapper of StringUtils.capitalize();
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
public class UCWords extends GenericUDF {

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
                String str = ((Text) converter.convert(arguments[0].get())).toString();
                return new Text(WordUtils.capitalize(str));
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
