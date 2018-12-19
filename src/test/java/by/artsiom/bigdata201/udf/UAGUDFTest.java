package by.artsiom.bigdata201.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class UAGUDFTest {
    private static final String UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36";

    @Rule
    public final transient ExpectedException expectedEx = ExpectedException.none();
    private UAGUDF uagudf;

    @Before
    public void setup() {
        uagudf = new UAGUDF();
    }

    @Test
    public void getDisplayString_shouldReturnCorrectDisplayString() {
        assertTrue(uagudf.getDisplayString(null).contains("user agent"));
    }

    @Test
    public void initialize_wrongArgumentsLength_shouldThrowException() throws UDFArgumentException {
        expectedEx.expect(UDFArgumentException.class);
        expectedEx.expectMessage("The argument list must be exactly 1 element.");

        uagudf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector
        });
    }

    @Test
    public void initialize_wrongArgumentsType_shouldThrowException() throws UDFArgumentException {
        expectedEx.expect(UDFArgumentException.class);
        expectedEx.expectMessage("The argument must be a string.");

        uagudf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
        });
    }

    @Test
    public void evaluate_null_shouldReturnNull() throws HiveException {
        uagudf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        });

        final Object result = uagudf.evaluate(new GenericUDF.DeferredObject[] {
                new GenericUDF.DeferredJavaObject(null)
        });

        assertNull(result);
    }

    @Test
    public void evaluate_validInput_shoudReturnCorrectResult() throws HiveException {
        final ObjectInspector resultInspector = uagudf.initialize(new ObjectInspector[] {
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        });

        final Object row = uagudf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(UA)
        });

        checkField((StandardStructObjectInspector) resultInspector, row, "DEVICE_TYPE", "Desktop");
        checkField((StandardStructObjectInspector) resultInspector, row, "PARENT", "Chrome 58.0");
    }

    private void checkField(StandardStructObjectInspector resultInspector, Object row, String fieldName, String expectedValue) {
        assertEquals(expectedValue, resultInspector.getStructFieldData(row, resultInspector.getStructFieldRef(fieldName)).toString());
    }
}
