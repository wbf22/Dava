package org.dava.external;

import org.dava.external.annotations.PrimaryKey;
import org.dava.external.annotations.Table;

@Table
public class OrderTable {

    @PrimaryKey
    private String orderId;
}
