CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY, 
  "name" varchar(160),
  "created_at" timestamp,
  "updated_at" timestamp,
  "country_code" int,
  "default_language" int
);