--HotItemWithSql pure sql impl
select * from (
    select
    *,
    row_number() over(partition by itemId order by cnt desc) as row_num
    from (
        select
        itemId,
        hop_end(ts, interval '5' minute, interval '1' hour),
        count(itemId) as cnt
        from datatable
        where behavior='pv'
        group by itemId,hop(ts, interval '5' minute, interval '1' hour)
    )
) where row_num <= 5