package org.dava.core.database.service.objects;

public class Transaction <T> {

    private String rollbackData;
    private T transactionResult;


    public Transaction(String rollbackData, T transactionResult) {
        this.rollbackData = rollbackData;
        this.transactionResult = transactionResult;
    }


    public String getRollbackData() {
        return rollbackData;
    }

    public T getTransactionResult() {
        return transactionResult;
    }
}
