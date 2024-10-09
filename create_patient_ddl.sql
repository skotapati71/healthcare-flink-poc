create table medical_insights
(
    id          int auto_increment primary key,
    medical_condition varchar(100),
    age_range varchar(20),
    patient_count int,
    window_start  timestamp,
    window_end    timestamp
);
