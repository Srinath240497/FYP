# Scatter plot 
> library(readr)
> View(benchmark_results)
> data <- read_csv("benchmark_results.csv")
> print(colnames(data))
> install.packages("tidyverse")
> library(ggplot2)
> View(data)
> ggplot(data, aes(x = pg_ms, y = mongo_ms)) +
  +     geom_point(color = "purple", size = 3, alpha = 0.7) +
  +     geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "red") + # 1:1 reference line
  +     labs(title = "Correlation: PostgreSQL vs MongoDB Time",
             +          subtitle = "Points above the red line indicate MongoDB was slower",
             +          x = "PostgreSQL Execution Time (ms)",
             +          y = "MongoDB Execution Time (ms)") +
  +     theme_minimal()


# Box Plot
> library(ggplot2)
> library(dplyr)
> df <- read.csv("query_performance_report.csv", stringsAsFactors = FALSE)
> colnames(df) <- trimws(colnames(df))
> print(colnames(df))
View(df)
> plot_data <- df %>% 
  +     filter(QueryType == "ColdVsWarm_SelectAll")
> ggplot(plot_data, aes(x = Database, y = ExecutionTimeMS, fill = Database)) +
  +     geom_boxplot(alpha = 0.7, outlier.shape = NA) +
  +     geom_jitter(width = 0.2, alpha = 0.5, size = 1.5) +
  +     scale_fill_manual(values = c("Postgres" = "#336791", "MongoDB" = "#47A248")) +
  +     theme_minimal() +
  +     labs(
    +         title = "Query Latency Distribution: PostgreSQL vs MongoDB",
    +         subtitle = "MSc Dissertation: Distributed Cloud Performance Analysis",
    +         x = "Database Engine",
    +         y = "Execution Time (ms)"
    +     ) +
  +     theme(legend.position = "none")



