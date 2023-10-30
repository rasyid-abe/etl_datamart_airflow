SELECT
	*
FROM
	M_Item_attribute
WHERE
	created_at 
    BETWEEN '{{ macros.ds_format(data_interval_start - macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
    AND '{{ macros.ds_format(data_interval_end - macros.timedelta(hours=7), "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d") }}'
