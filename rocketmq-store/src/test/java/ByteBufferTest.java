import com.alibaba.rocketmq.store.MapedFile;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by medusar on 2016/7/26.
 */
public class ByteBufferTest {

    @Test
    public void testByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBuffer newBuffer = buffer.slice();
//        newBuffer.position(1);
        newBuffer.putLong(100L);

        System.out.println(buffer.position());
        System.out.println(buffer.limit());
        System.out.println(buffer.capacity());

        System.out.println(newBuffer.position());
        System.out.println(newBuffer.limit());
        System.out.println(newBuffer.capacity());


        ByteBuffer slice = newBuffer.slice();
        slice.putLong(1L);


        System.out.println(newBuffer.position());
        System.out.println(newBuffer.limit());
        System.out.println(newBuffer.capacity());

        System.out.println(slice.position());
        System.out.println(slice.limit());
        System.out.println(slice.capacity());


//        newBuffer.flip();
//        long aLong = newBuffer.getLong();
//        System.out.println(aLong);
//
//        System.out.println(newBuffer.position());
//        System.out.println(newBuffer.limit());
//        System.out.println(newBuffer.capacity());
//
//        newBuffer.flip();
//        newBuffer.putLong(200L);
//
//        System.out.println(newBuffer.position());
//        System.out.println(newBuffer.limit());
//        System.out.println(newBuffer.capacity());
    }


    @Test
    public void testViewedBuffer() throws IOException {
        File file = new File("./unit_test_store/10086");

        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        MappedByteBuffer mappedByteBuffer = accessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024);

        MapedFile.clean(mappedByteBuffer);

    }


}
