(ns metabase.test.data.sqlserver
  "Code for creating / destroying a SQLServer database from a `DatabaseDefinition`."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.sql :as sql.tx]
            [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
            [metabase.util.i18n :refer [trs]])
  (:import (java.sql Connection ResultSet)))

(sql-jdbc.tx/add-test-extensions! :sqlserver)

(doseq [[base-type database-type] {:type/BigInteger     "BIGINT"
                                   :type/Boolean        "BIT"
                                   :type/Date           "DATE"
                                   :type/DateTime       "DATETIME"
                                   :type/DateTimeWithTZ "DATETIMEOFFSET"
                                   :type/Decimal        "DECIMAL"
                                   :type/Float          "FLOAT"
                                   :type/Integer        "INTEGER"
                                   ;; TEXT is considered deprecated -- see
                                   ;; https://msdn.microsoft.com/en-us/library/ms187993.aspx
                                   :type/Text           "VARCHAR(1024)"
                                   :type/Time           "TIME"}]
  (defmethod sql.tx/field-base-type->sql-type [:sqlserver base-type] [_ _] database-type))


(defmethod tx/dbdef->connection-details :sqlserver
  [_ context {:keys [database-name]}]
  {:host     (tx/db-test-env-var-or-throw :sqlserver :host "localhost")
   :port     (Integer/parseInt (tx/db-test-env-var-or-throw :sqlserver :port "1433"))
   :user     (tx/db-test-env-var-or-throw :sqlserver :user "SA")
   :password (tx/db-test-env-var-or-throw :sqlserver :password "P@ssw0rd")
   :db       (when (= context :db)
               database-name)})

(defmethod sql.tx/drop-db-if-exists-sql :sqlserver
  [_ {:keys [database-name]}]
  ;; Kill all open connections to the DB & drop it
  (apply format "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'%s')
                 BEGIN
                     ALTER DATABASE \"%s\" SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                     DROP DATABASE \"%s\";
                 END;"
         (repeat 3 database-name)))

(defmethod sql.tx/drop-table-if-exists-sql :sqlserver
  [_ {:keys [database-name]} {:keys [table-name]}]
  (let [db-name database-name]
    (format "IF object_id('%s.dbo.%s') IS NOT NULL DROP TABLE \"%s\".dbo.\"%s\";" db-name table-name db-name table-name)))

(defn- server-spec []
  (sql-jdbc.conn/connection-details->spec :sqlserver
    (tx/dbdef->connection-details :sqlserver :server nil)))

(defn- database-exists? [database-name]
  (seq (jdbc/query (server-spec) (format "SELECT name FROM master.dbo.sysdatabases WHERE name = N'%s'" database-name))))

(defmethod sql.tx/qualified-name-components :sqlserver
  ([_ db-name]                       [db-name])
  ([_ db-name table-name]            [db-name "dbo" table-name])
  ([_ db-name table-name field-name] [db-name "dbo" table-name field-name]))

(defmethod sql.tx/pk-sql-type :sqlserver [_] "INT IDENTITY(1,1)")

(defmethod tx/aggregate-column-info :sqlserver
  ([driver ag-type]
   (merge
    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type)
    (when (#{:count :cum-count} ag-type)
      {:base_type :type/Integer})))

  ([driver ag-type field]
   (merge
    ((get-method tx/aggregate-column-info ::tx/test-extensions) driver ag-type field)
    (when (#{:count :cum-count} ag-type)
      {:base_type :type/Integer}))))

;; SQL server only supports setting holdability at the connection level, not the statement level, as per
;; https://docs.microsoft.com/en-us/sql/connect/jdbc/using-holdability?view=sql-server-ver15
;; and
;; https://github.com/microsoft/mssql-jdbc/blob/v9.2.1/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5349-L5357
;; an exception is thrown if they do not match, so it's safer to simply NOT try to override it at the statement level,
;; because it's not supported anyway
(defmethod sql-jdbc.execute/prepared-statement :sqlserver
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn
                                sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (try
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
        (catch Throwable e
          (log/debug e (trs "Error setting prepared statement fetch direction to FETCH_FORWARD"))))
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; similar rationale to prepared-statement above
(defmethod sql-jdbc.execute/statement :sqlserver
  [_ ^Connection conn]
  (let [stmt (.createStatement conn
                               ResultSet/TYPE_FORWARD_ONLY
                               ResultSet/CONCUR_READ_ONLY)]
    (try
      (try
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
        (catch Throwable e
          (log/debug e (trs "Error setting statement fetch direction to FETCH_FORWARD"))))
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))
