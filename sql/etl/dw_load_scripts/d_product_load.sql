INSERT INTO dw.d_product (
    product_id,
    product_name, 
    product_category, 
    product_subcategory
)
SELECT 
    product_id, 
    product_name, 
    category_name, 
    subcategory_name
FROM sta.sta_sr_products tb1,
    sta.sta_sr_subcategory tb2, 
    sta.sta_sr_categories tb3
WHERE tb3.category_id = tb2.category_id
    AND tb2.subcategory_id = tb1.subcategory_id;