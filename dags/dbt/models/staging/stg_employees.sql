SELECT
    employe_id as employee_id,
    branch_id,
    salary,
    to_date(join_date, 'YYYY-MM-DD') as join_date,
    case
        when resign_date = '' then NULL
        else to_date(resign_date, 'YYYY-MM-DD') 
    end as resign_date
from {{ source('dbt','raw_employees') }}