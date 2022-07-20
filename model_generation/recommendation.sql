use tripadvisor_london;

update service_entity set votes = 
case 
	when replace(replace(replace(replace(replace(review_count,'reviews',''),'review',''),',',''),' ',''),'English','') != ''
	then cast(
	replace(replace(replace(replace(replace(review_count,'reviews',''),'review',''),',',''),' ',''),'English','')
	as SIGNED INTEGER)
	else
        0
end 

