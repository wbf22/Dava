package org.dava.core.database.service;

import org.dava.core.database.objects.database.structure.Database;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;



public class OperationService {

    String s =
        """
            SELECT p.product_id AS productId, p. sku, p.name, p.description, p.wholesale_cost AS wholesaleCost, p.msrp, 
                   p.ticket_duration_in_days AS ticketDurationInDays, 
                   p.age_range_title AS ageRangeTitle, p.age_range_description AS ageRangeDescription, p.tier, 
                   p.terms_of_use as termsOfUse, p.product_identifier as productIdentifier
               FROM products.product p 
               WHERE p.pricelist_version_id IN
                      (
                        SELECT pv.pricelist_version_id
                           FROM attractions.attraction a
                             INNER JOIN products.pricelist_version pv
                               ON a.attraction_identifier  = pv.attraction_identifier
                           WHERE upper(a.attraction_identifier) = upper(:attraction_identifier_filter)
                           AND p.is_active = true
                           AND :date BETWEEN pv.start_dts_utc AND pv.end_dts_utc
                           ORDER BY pv.priority DESC
                           LIMIT 1
                      )
        """;



    private Database database;


    public Map<String, String> select(Map<String, String> fieldToValues) {
        return null;
    }


    public List<Map<String, String>> where(List<Map<String, String>> rows, Predicate<Map<String, String>> lambdaFilter) {
        return rows.stream()
            .filter(lambdaFilter)
            .toList();
    }


    public Map<String, String> join(Map<String, String> firstValue, Map<String, String> secondValue) {
        firstValue.putAll(secondValue);
        return firstValue;
    }


}
