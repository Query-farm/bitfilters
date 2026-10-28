create table series_data as (select hash(id) as hashed_id from generate_series(1, 10000000) as id);

-- create table quotient_filters as (
--   select id % 2 as remainder, quotient_filter(15, 1, hash(id)) as filter
--   from series_data
--   group by id % 2
-- );


-- select remainder, octet_length(filter) from quotient_filters;

-- select remainder,
-- count(case when quotient_filter_contains(filter, hash(id)) then 1 else null end) as is_contained
-- from series_data, quotient_filters
-- where
-- series_data.id % 2 == quotient_filters.remainder
-- group by remainder;

-- select remainder, count(case when quotient_filter_contains(filter, hash(id)) then 1 else null end) as is_contained
-- from series_data, quotient_filters where series_data.id % 2 != quotient_filters.remainder group by remainder;


create table xor_filters as (
  select hashed_id % 2 as remainder,
  xor16_filter(hashed_id) as xor16_filter,
  xor8_filter(hashed_id) as xor8_filter
  from series_data
  group by hashed_id % 2
);

select remainder,
octet_length(xor16_filter) as xor16_length,
  octet_length(xor8_filter) as xor8_length
 from xor_filters;


select remainder,
count(case when xor16_filter_contains(xor16_filter, hashed_id) then 1 else null end) as xor16_is_contained,
count(case when xor8_filter_contains(xor8_filter, hashed_id) then 1 else null end) as xor8_is_contained
from series_data, xor_filters
where
series_data.hashed_id % 2 == xor_filters.remainder
group by remainder;


-- Check for false positives
select remainder,
count(case when xor16_filter_contains(xor16_filter, hashed_id) then 1 else null end) as xor16_false_positives,
count(case when xor8_filter_contains(xor8_filter, hashed_id) then 1 else null end) as xor8_false_positives
from series_data, xor_filters where series_data.hashed_id % 2 != xor_filters.remainder group by remainder;

--select count(case when xor16_filter_contains(xor16_filter, hashed_id) then 1 else null end) from series_data, xor_filters where series_data.hashed_id % 2 != xor_filters.remainder;


create table binary_fuse_filters as (
  select hashed_id % 2 as remainder,
  binary_fuse16_filter(hashed_id) as binary_fuse16_filter,
  binary_fuse8_filter(hashed_id) as binary_fuse8_filter
  from series_data
  group by hashed_id % 2
);


select remainder,
octet_length(binary_fuse16_filter) as binary_fuse16_length,
  octet_length(binary_fuse8_filter) as binary_fuse8_length
 from binary_fuse_filters;


select remainder,
count(case when binary_fuse16_filter_contains(binary_fuse16_filter, hashed_id) then 1 else null end) as binary_fuse16_is_contained,
count(case when binary_fuse8_filter_contains(binary_fuse8_filter, hashed_id) then 1 else null end) as binary_fuse8_is_contained
from series_data, binary_fuse_filters
where
series_data.hashed_id % 2 == binary_fuse_filters.remainder
group by remainder;


-- Check for false positives
select remainder,
count(case when binary_fuse16_filter_contains(binary_fuse16_filter, hashed_id) then 1 else null end) as binary_fuse16_false_positives,
count(case when binary_fuse8_filter_contains(binary_fuse8_filter, hashed_id) then 1 else null end) as binary_fuse8_false_positives
from series_data, binary_fuse_filters where series_data.hashed_id % 2 != binary_fuse_filters.remainder group by remainder;
