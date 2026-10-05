ALTER TABLE attempts ADD COLUMN process_pid INTEGER CHECK (process_pid IS NULL OR process_pid > 1);
ALTER TABLE attempts ADD COLUMN process_start TEXT CHECK ((process_pid IS NULL) = (process_start IS NULL));
