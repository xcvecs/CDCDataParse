package top.byteinfo.iter;

import top.byteinfo.iter.schema.DataBase;

import java.sql.ResultSet;

public enum GlobalConstant {
    BEGIN;

    /**
     *
     * @see ResultSet#getString(String)
     *
     */
    public enum SQLResultColumnLabel {


        /**
         * @see SchemaCapture#capture()
         */
        SCHEMA_NAME,
        DEFAULT_CHARACTER_SET_NAME,

        /**
         * @see SchemaCapture#captureDatabase(DataBase)
         */
//        TABLE_NAME,
        TABLE_NAME,
        CHARACTER_SET_NAME,

        /**
         *
         */
        COLUMN_NAME,
        DATA_TYPE,
//        CHARACTER_SET_NAME,
        ORDINAL_POSITION,
        COLUMN_TYPE,
        DATETIME_PRECISION,
//        COLUMN_TYPE,
COLUMN_KEY,
        PRI,

        /**
         *
         */
//        ORDINAL_POSITION,
//        TABLE_NAME,
//        COLUMN_NAME,
        ;
        public String o(){
            return name();
        }





    }
}
