import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class GenericDeserializableAvro {

    public static void main(String[] args) throws IOException {

        ClassLoader classLoader = GenericDeserializableAvro.class.getClassLoader();


        Schema schema = new Schema.Parser().parse(new File(classLoader.getResource("user-1.avsc").getFile()));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File("users.avro"), datumReader);
        GenericRecord user = null;

        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
