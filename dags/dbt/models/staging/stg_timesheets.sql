SELECT
    timesheet_id,
    employee_id,
    case
        when date = '' then NULL
        else to_date(date, 'YYYY-MM-DD') 
    end as date,
    case
        WHEN checkin = '' then NULL
        else cast(checkin as time)
    end as check_in,
    case
        WHEN checkout = '' then NULL
        else cast(checkout as time)
    end as check_out


from {{ source('dbt','raw_timesheets') }}