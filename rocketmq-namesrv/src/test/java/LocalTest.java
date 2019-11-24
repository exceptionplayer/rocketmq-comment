import com.alibaba.rocketmq.common.constant.PermName;
import org.junit.Test;

/**
 * Created by medusar on 2016/7/20.
 */
public class LocalTest {

    @Test
    public void testAnd() {
        int perm = 100;
        perm &= ~PermName.PERM_WRITE;
        perm &= ~PermName.PERM_WRITE;
        System.out.println(perm);
    }

}