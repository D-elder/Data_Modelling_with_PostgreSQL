{{ config(
    partition_by={
      "field": "DateApproved",
      "data_type": "date"
    }
)}}
with source as(
    select
        s."LoanNumber" as id,
        l.Lender_id as Lender_id,
        s."Term" as  Term,
        date(s."DateApproved") as DateApproved,
        date(s."ForgivenessDate") as DateForgived,
        pm.id as processingmethod_id,
        b.Borrower_id as borrower_id,
        s."SBAGuarantyPercentage" as SBAGuarantyPercentage,
        s."InitialApprovalAmount" as InitialApprovalAmount,
        s."CurrentApprovalAmount" as CurrentApprovalAmount,
        s."UndisbursedAmount" as UndisbursedAmount,
        bage.id as businessage_id,
        s."JobsReported" as JobsReported,
        btype."type_id" as businesstype_id,
        s."ForgivenessAmount" as ForgivenessAmount,
        lo.id as loanstatus_id,
        current_timestamp as insertion_timestamp
    from {{ ref('stg_SBAdata') }} s
    left join {{ ref('borrowers') }} b
    on b.Borrower_id = s."LoanNumber"
    left join {{ ref('lenders') }} l
    on l.Lender_id = s."ServicingLenderLocationID"
    left join {{ ref('loanstatus') }} lo
    on lo.id = s."LoanNumber"
    left join {{ ref('businessage') }} bage
    on bage.id = s."LoanNumber"
    left join {{ ref('businesstype') }} btype
    on btype.type_id = s."LoanNumber"
    left join {{ ref('processingmethod') }} pm
    on pm.id = s."LoanNumber"
    where s."LoanNumber" is not null
),
unique_source as (
    select *,
            row_number() over(partition by id, Lender_id, borrower_id, businesstype_id, businessage_id, loanstatus_id,processingmethod_id, DateApproved) as row_number
    from source
)
select 
   id,
   Lender_id,
   Term,
   DateApproved,
   DateForgived,
   processingmethod_id,
   borrower_id,
   SBAGuarantyPercentage,
   InitialApprovalAmount,
   CurrentApprovalAmount,
   UndisbursedAmount,
   businessage_id,
   JobsReported,
   businesstype_id,
   ForgivenessAmount,
   loanstatus_id,
   insertion_timestamp
from unique_source
where row_number = 1