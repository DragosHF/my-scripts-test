select
    date_string_backwards
    , hellofresh_week as hf_week
from dimensions.date_dimension
where date_string_backwards >= '2018-01-01'