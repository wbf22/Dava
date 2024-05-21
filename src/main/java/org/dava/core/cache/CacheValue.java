package org.dava.core.cache;

import org.dava.api.annotations.PrimaryKey;
import org.dava.api.annotations.Table;

@Table()
public class CacheValue {
    
    @PrimaryKey
    private String key;
    private String value;

    
    public CacheValue(String key, String value) {
        this.key = key;
        this.value = value;
    }


    public String getKey() {
        return key;
    }


    public void setKey(String key) {
        this.key = key;
    }


    public String getValue() {
        return value;
    }


    public void setValue(String value) {
        this.value = value;
    }


    

    

}
