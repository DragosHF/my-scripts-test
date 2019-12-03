select
    po.po_id as po_number
    , po.supplier
    , pl.prod_code as sku_code
    , po.delivery_start_time as delivery_date_time_start
    , po.delivery_end_time as delivery_date_time_end
    , pl.original_expected_qty as total_ordered_units
    , dl1.delivery_date as actual_delivery_date_time
    , pl.accepted_qty as total_received_units
    , s.rejected_qty as rejected_units
    , qc.out_of_spec_qty as out_of_spec_units
from mis.mx_po po
left join mis.mx_po_line pl
    on po.po_id = pl.po_id
left join
    (
        select
            d.po_id
            , dl.prod_code
            , max(d.delivery_date) delivery_date
        from mis.mx_delivery_line dl
        join mis.mx_delivery d
            on dl.delivery_id = d.delivery_id
        group by 1,2
    ) dl1
on pl.po_id = dl1.po_id
    and pl.prod_code = dl1.prod_code
left join
    (
        select
            po_id
            , prod_code
            , sum(from_value-to_value) rejected_qty
        from mis.mi_stock
        where stock_sla_reason = 'Ware beschaedigt'
        group by 1,2
    ) s
    on pl.po_id = s.po_id
        and pl.prod_code = s.prod_code
left join
    (
        select
            cast(regexp_substr(sample_delivery_id, '^.+_[EO]\\d+', 1) as char) po_id
            , prod_code
            , sum(vm_check_value) out_of_spec_qty
        from mis.mi_qc_result_sample
        where vm_check_description = 'OTIFIQ'
        group by 1,2
    ) qc
    on po.po_id = qc.po_id
        and pl.prod_code = qc.prod_code
        and qc.po_id is not null
where left(pl.prod_code,3) not in ('VPM', 'VBM', 'OTH', 'SRY')
    and po.po_type != 'INTERNAL PO'
