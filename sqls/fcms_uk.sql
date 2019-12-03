select
    -- get po attributes
    po.po_id as po_number
    , po.supplier
    -- get po line attributes
    , pl.prod_code as sku_code
    , dl1.prod_description
    , pl.original_expected_qty
    -- get delivery attributes
    , min(d.delivery_start_time) as delivery_date_time_start
    , max(d.delivery_end_time) as delivery_date_time_end
    , max(d.delivery_date) as actual_delivery_date_time
    -- get delivery line attributes
    , sum(dl1.received_qty) as received_qty
    , sum(dl1.palletised_usable_qty) as palletised_usable_qty
    , sum(dl2.rejected_qty) as rejected_qty
from mis.mx_po po
left join mis.mx_po_line pl
    on po.po_id = pl.po_id
left join mis.mx_delivery d
    on po.po_id = d.po_id
left join mis.mx_delivery_line dl1
    on d.delivery_id = dl1.delivery_id
        and pl.prod_code = dl1.prod_code
left join mis.mx_delivery_line dl2
    on d.delivery_id = dl2.delivery_id
        and pl.prod_code = dl2.prod_code
        and dl2.reject_reason != ''
where left(pl.prod_code, 3) not in ('PCK')
    and not ((dl1.received_qty = 0 and dl1.palletised_usable_qty > 0)) -- to exclude errors when we palletise without having received anything
    and po.po_type != 'INTERNAL PO'
    and regexp_like(po.po_id, '^.+_O\\d$') = TRUE  -- only regular POs
group by 1,2,3,4,5