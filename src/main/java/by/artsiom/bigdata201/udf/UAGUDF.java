package by.artsiom.bigdata201.udf;

import com.blueconic.browscap.*;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Description(
        name = "UAGUDF",
        value = "_FUNC_(str) - parses any user agent string into separate fields.",
        extended = "Example:" +
                "> SELECT uagudf(useragent).DEVICE_TYPE, \n" +
                "         uagudf(useragent).PLATFORM_DESCRIPTION, \n" +
                "         uagudf(useragent).PARENT \n" +
                "  FROM   clickLogs;\n" +
                "  +---------------+-----------------------------+------------------------+\n" +
                "  |  device_type  |     platform_description    |          parent        |\n" +
                "  +---------------+-----------------------------+------------------------+\n" +
                "  | Phone         | Android 6.0                 | Chrome 46              |\n" +
                "  | Tablet        | Android 5.1                 | Chrome 40              |\n" +
                "  | Desktop       | Linux Intel x86_64          | Chrome 59              |\n" +
                "  | Game Console  | Windows 10.0                | Edge 13                |\n" +
                "  +---------------+-----------------------------+------------------------+\n" +
                "  For more field names please take a look at " + 
                "https://github.com/blueconic/browscap-java/blob/master/src/main/java/com/blueconic/browscap/BrowsCapField.java"
)
public class UAGUDF extends GenericUDF {
    private static List<String> fieldNames;
    private static UserAgentParser uaParser;

    private StringObjectInspector useragentOI;

    @Override
    public ObjectInspector initialize(final ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("The argument list must be exactly 1 element.");
        }

        final ObjectInspector oi = arguments[0];
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE || !(oi instanceof StringObjectInspector)) {
            throw new UDFArgumentException("The argument must be a string.");
        }
        useragentOI = (StringObjectInspector) oi;

        initializeParser();

        final List<ObjectInspector> fieldObjectInspectors = fieldNames
                .stream()
                .map(field -> PrimitiveObjectInspectorFactory.writableStringObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
    }

    @Override
    public Object evaluate(final DeferredObject[] args) throws HiveException {
        return Optional.ofNullable(useragentOI.getPrimitiveJavaObject(args[0].get()))
                .map(uaString ->
                        uaParser.parse(uaString)
                                .getValues()
                                .entrySet()
                                .stream()
                                .map(entry -> new Text(entry.getValue()))
                                .toArray())
                .orElse(null);
    }

    @Override
    public String getDisplayString(final String[] children) {
        return "Parses the user agent string into all possible pieces.";
    }

    private static synchronized void initializeParser() throws UDFArgumentException {
        if (uaParser == null) {
            final List<BrowsCapField> fields = Arrays.asList(BrowsCapField.values());
            try {
                uaParser = new UserAgentService().loadParser(fields);
            } catch (IOException | ParseException e) {
                throw new UDFArgumentException("Cannot initialise the ua parser.");
            }
            fieldNames = fields
                    .stream()
                    .map(BrowsCapField::name)
                    .collect(Collectors.toList());
        }
    }
}
