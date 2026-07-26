ALTER TABLE volumes
ADD CONSTRAINT volumes_watermarks_ordered CHECK (published_sequence <= durable_sequence AND durable_sequence <= local_sequence) NOT VALID;

ALTER TABLE volumes VALIDATE CONSTRAINT volumes_watermarks_ordered;
