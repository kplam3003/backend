-- VOC 11 profile stats
distinct_reviews AS (
    SELECT DISTINCT
      company_name,
      source_name,
      CASE 
        WHEN url IS NULL THEN "blank"
        ELSE url
      END url,
      parent_review_id,
      MAX(rating) AS rating,
      MAX(review_date) AS review_date
    FROM voc_summary_table
    GROUP BY company_name, source_name, url, parent_review_id
),
all_time_table AS (
  SELECT
    company_name,
    source_name,
    url,
    COUNT(parent_review_id) AS reviews_all_time,
    AVG(rating) AS rating_all_time
  FROM distinct_reviews
  GROUP BY company_name, source_name, url
),
review_stats_table AS (
  SELECT
    company_name,
    source_name,
    url,
    MAX(total_reviews) as total_reviews_stats,
    MAX(total_ratings) as total_ratings_stats,
    MAX(average_rating) as average_rating_stats
  FROM voc_summary_table
  GROUP BY company_name, source_name, url
),