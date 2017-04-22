package backtype.storm.topology;

import java.io.Serializable;
import java.util.Map;

/*
 * This interface is used to notify the update of user configuration
 * for bolt and spout 
 */
//finished~
public interface IDynamicComponent extends Serializable {
    void update(Map conf);
}