package org.dava.api;

import org.dava.api.annotations.Query;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.MarshallingService;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.objects.conditions.Equals;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import javax.xml.crypto.Data;

import static org.dava.core.common.Checks.safeCastParameterized;
import static org.dava.core.database.objects.exception.ExceptionType.REPOSITORY_ERROR;


public class Repository<T, ID> {

    private Database database;
    private Table<T> table;



    public Repository (Database database){
        this.database = database;
        String tableName = this.getClass().getTypeParameters()[0].getClass().getName();
        this.table = safeCastParameterized(database.getTableByName(tableName), Table.class);
    }



    public T findById(ID primaryKey) {
        return null;
    }

    public List<T> findByColumn(String columnName, String value) {
        
        Equals equals = new Equals(columnName, value);
        
        return equals.retrieve(table, List.of(), null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .toList();
    }


    public List<T> findAll() {
        return null;
    }


    public List<T> query(Map<String, String> params) {
        String query = getQueryFromCaller(
            Thread.currentThread().getStackTrace()
        );
        return null;
    }

    public List<T> findByProvidedFields(T objectWithSomeFields) {
        return null;
    }

    public void save(T row) {

    }

    public void saveAll(List<T> rows) {

    }

    public void delete(ID primaryKey) {

    }

    public void deleteAll(List<ID> primaryKeys) {

    }

    private String getQueryFromCaller(StackTraceElement[] stackTrace) {
        Method callingMethod = getCallingMethod(stackTrace);
        if (callingMethod != null) {
            Query queryAnnotation = callingMethod.getAnnotation(Query.class);
            if (queryAnnotation != null) {
                return queryAnnotation.query();
            }
        }
        throw new DavaException(REPOSITORY_ERROR, "Tried to invoke Repository<T, ID>.query() " +
                                    "in a method without an @Query annotation", null);
    }

    private Method getCallingMethod(StackTraceElement[] stackTrace) {
        StackTraceElement element = stackTrace[2];
        Class<?> callerClass = this.getClass();
        Method[] methods = callerClass.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals(element.getMethodName())) {
                return method;
            }
        }
        return null;
    }


}
