select
       # get po attributes
    po.po_id as po_number
    , po.supplier
    , po.delivery_start_time
    , po.delivery_end_time
     # get po line attributes
    , pl.prod_code as sku_code
    , pl.original_expected_qty
     # get delivery attributes
    , max(d.delivery_date) actual_delivery_date_time
     # get delivery line attributes
    , sum(dl.received_qty) received_qty
    , sum(dl.palletised_usable_qty) palletised_usable_qty
    , sum(dl.rejected_qty) rejected_qty
     # get the out of spec quantities
    , sum(qc.vm_check_value) out_of_spec_qty
from mis.mx_po po
LEFT JOIN mis.mx_po_line pl
    on po.po_id = pl.po_id
LEFT JOIN mis.mx_delivery d
    on po.po_id = d.po_id
LEFT JOIN mis.mx_delivery_line dl
    on d.delivery_id = dl.delivery_id
    and pl.prod_code = dl.prod_code
LEFT JOIN mis.mi_qc_result_sample qc
    on dl.delivery_line_id = qc.delivery_line_id
    and qc.vm_check_description = 'OTIFIQ'
where date(d.delivery_start_time) >= %(d_start)s # for a specific time frame
and left(pl.prod_code,3) not in ('VPM', 'VBM', 'OTH', 'SRY')
and not((dl.received_qty = 0 and dl.palletised_usable_qty > 0)) # to exclude errors when we palletise without having received anything
and po.po_type != 'INTERNAL PO'
group by 1,2,3,4,5,6