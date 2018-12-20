package by.artsiom.bigdata201.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

@Description(
        name = "UAGUDF",
        value = "_FUNC_(str) - parses any user agent string into separate fields (os, device, browser).",
        extended = "Example:" +
                "> SELECT uagudf(useragent).device, \n" +
                "         uagudf(useragent).os, \n" +
                "         uagudf(useragent).browser \n" +
                "  FROM   clickLogs;\n" +
                "  +---------------+-----------------------------+------------------------+\n" +
                "  |     device    |               os            |         browser        |\n" +
                "  +---------------+-----------------------------+------------------------+\n" +
                "  | Phone         | Android 6.0                 | Chrome 46              |\n" +
                "  | Tablet        | Android 5.1                 | Chrome 40              |\n" +
                "  | Desktop       | Linux Intel x86_64          | Chrome 59              |\n" +
                "  | Game Console  | Windows 10.0                | Edge 13                |\n" +
                "  +---------------+-----------------------------+------------------------+\n" 
)
public class UAGUDF extends GenericUDF {
    private static String BROWSER = "browser";
    private static String OS = "os";
    private static String DEVICE = "device";

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

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Arrays.asList(BROWSER, OS, DEVICE),
                Collections.nCopies(3, PrimitiveObjectInspectorFactory.writableStringObjectInspector)
        );
    }

    @Override
    public Object evaluate(final DeferredObject[] args) throws HiveException {
        return Optional.ofNullable(useragentOI.getPrimitiveJavaObject(args[0].get()))
                .map(uaString -> {
                    UserAgent ua = UserAgent.parseUserAgentString(uaString);
                    return new Text[] {
                            new Text(ua.getBrowser().getName()),
                            new Text(ua.getOperatingSystem().getName()),
                            new Text(ua.getOperatingSystem().getDeviceType().getName())
                    };
                })
                .orElse(null);
    }

    @Override
    public String getDisplayString(final String[] children) {
        return "Parses the user agent string into OS, Browser, and Device pieces.";
    }
}
