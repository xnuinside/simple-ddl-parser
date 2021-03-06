CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  "country_code" int,
  "default_language" int
);

CREATE TABLE "languages" (
  "id" int PRIMARY KEY,
  "code" varchar(2) NOT NULL,
  "name" varchar NOT NULL
);

CREATE TABLE "countries" (
  "id" int PRIMARY KEY,
  "code" varchar(4) NOT NULL,
  "name" varchar NOT NULL
);

CREATE TABLE "path_owners" (
  "user_id" int,
  "path_id" int,
  "type" int DEFAULT 1
);

CREATE TABLE "paths" (
  "id" int PRIMARY KEY,
  "title" varchar NOT NULL,
  "description" varchar(160),
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "path_steps" (
  "step_id" int,
  "path_id" int
);

CREATE TABLE "steps" (
  "id" int PRIMARY KEY,
  "title" varchar NOT NULL,
  "description" varchar(160),
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "step_materials" (
  "path_id" int,
  "step_id" int
);

CREATE TABLE "materials" (
  "id" int PRIMARY KEY,
  "title" varchar NOT NULL,
  "description" varchar,
  "link" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE "material_attachments" (
  "material_id" int,
  "attachment_id" int
);

CREATE TABLE "attachments" (
  "id" int PRIMARY KEY,
  "title" varchar,
  "description" varchar,
  "created_at" timestamp,
  "updated_at" timestamp
);

ALTER TABLE "users" ADD FOREIGN KEY ("country_code") REFERENCES "countries" ("id");

ALTER TABLE "users" ADD FOREIGN KEY ("default_language") REFERENCES "languages" ("id");

ALTER TABLE "path_owners" ADD FOREIGN KEY ("user_id") REFERENCES "users" ("id");

ALTER TABLE "path_owners" ADD FOREIGN KEY ("path_id") REFERENCES "paths" ("id");

ALTER TABLE "path_steps" ADD FOREIGN KEY ("step_id") REFERENCES "steps" ("id");

ALTER TABLE "path_steps" ADD FOREIGN KEY ("path_id") REFERENCES "paths" ("id");

ALTER TABLE "step_materials" ADD FOREIGN KEY ("path_id") REFERENCES "materials" ("id");

ALTER TABLE "step_materials" ADD FOREIGN KEY ("step_id") REFERENCES "steps" ("id");

ALTER TABLE "material_attachments" ADD FOREIGN KEY ("material_id") REFERENCES "materials" ("id");

ALTER TABLE "material_attachments" ADD FOREIGN KEY ("attachment_id") REFERENCES "attachments" ("id");
