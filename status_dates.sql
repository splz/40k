with
owners as
(select owner_id, owner_name, owner_email, owner_department, owner_role, active
from
 (select hre.user_id as owner_id,
       hre.name as owner_name,
       ru.login as owner_email,
       hrd.complete_name as owner_department,
       hrj.name as owner_role,
       hre.active as active,
       row_number() over (partition by hre.user_id order by hre.write_date desc) as dupes
from hr_employee hre
left join res_users ru on ru.id = hre.user_id
left join hr_department hrd on hrd.id = hre.department_id
left join hr_job hrj on hrj.id = hre.job_id
where user_id is not null and  ru.login like '%@%') with_duplicates
where with_duplicates.dupes = 1),

dates as
        (
        select distinct mm.res_id,
                min(mm.create_date) as date_created,
                max(mm.create_date) filter ( where mm.subtype_id = 7) as date_won,
                max(mm.create_date) filter ( where mm.subtype_id = 8) as date_lost,
                max(mm.create_date) filter ( where mm.subtype_id = any(array[6,24])) as date_reopened
        from mail_message mm
        join mail_message_subtype mms on mm.subtype_id = mms.id
        where mm.model = 'crm.lead'
          and mm.message_type = 'notification'
          and not mms.name = any(array['Note', 'Activities'])
        group by mm.res_id
        ),

dates_agg as
        (select res_id,
               date_created,
               greatest(date_won, date_lost, date_reopened) status_date
        from dates
        group by res_id, date_created, date_reopened, date_won, date_lost),

status_dates as
        (select da.*,
               case
                   when greatest(d.date_won,d.date_reopened,d.date_lost) = d.date_won then 'won'
                   when greatest(d.date_won,d.date_reopened,d.date_lost) = d.date_lost then 'lost'
                   when greatest(d.date_won,d.date_reopened,d.date_lost) = d.date_reopened then 'open'
                   else 'open' end as status
        from dates_agg da
        left join dates d on da.res_id = d.res_id and da.status_date = any(array[d.date_won,d.date_lost, d.date_reopened])),

dataset as
(select cl.create_date::date,
       to_char(cl.create_date, 'IYIW') as week_num,
       cl.user_id,
       coalesce(emp.owner_role, 'No job title') as owner_role,
       cl.id,
       cl.name,
       cl.uom_quantity as qty_loads,
       cl.partner_id as related_account_id,
       cl.selling_price,
       cl.type,
       cl.state as status,
       lr.name as loss_reason,
       unnest(coalesce(array_agg(ctr.tag_id) filter ( where ctr.tag_id <> 142), '{142}')) as tag_id,
       split_part(unnest(coalesce(array_agg(ct.name) filter ( where ct.name <> '40K' ), '{40K}')),'_',1) as tag,
       case when selling_price > 0 and cl.uom_quantity > 1 then 'yes' else 'no' end as filtered
from crm_lead cl
left join crm_tag_rel ctr on cl.id = ctr.lead_id
left join crm_tag ct on ctr.tag_id = ct.id
left join owners emp on emp.owner_id = cl.user_id
left join crm_lost_reason lr on cl.lost_reason = lr.id
where ctr.tag_id in (select id from crm_tag where name like '%40K%') and cl.create_date <= '2023-08-29'::date
group by cl.create_date::date,
       to_char(cl.create_date, 'IYIW'),
       cl.user_id,
       coalesce(emp.owner_role, 'No job title'),
       cl.id,
       cl.name,
       cl.uom_quantity,
       cl.partner_id,
       cl.selling_price,
       cl.type,
       cl.state,
       lr.name
order by cl.create_date)

select d.*,ds.status from dataset ds
join  status_dates d on d.res_id = ds.id
where ds.status <> d.status;
