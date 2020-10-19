using System;
using System.Data.Common;
//using System.Data.SqlClient;

namespace Atomus.Database
{
    public class Influx : IDatabase
    {
        InfluxDataAdapter sqlDataAdapter;
        
        public Influx()
        {
            DbCommand dbCommand;
            DbConnection dbConnection;

            this.sqlDataAdapter = new InfluxDataAdapter();

            dbCommand = new InfluxDbCommand();

            dbConnection = new InfluxDbConnection();
            ((InfluxDbConnection)dbConnection).dbCommand = dbCommand;

            dbCommand.Connection = dbConnection;

            this.sqlDataAdapter.SelectCommand = dbCommand;
        }

        DbParameter IDatabase.AddParameter(string parameterName, DbType dbType, int size)
        {
            //DbCommand sqlCommand;

            try
            {
                //sqlCommand = this.sqlDataAdapter.SelectCommand;

                //if (size == 0)
                //    return ((SqlParameterCollection)sqlCommand.Parameters).Add(parameterName, this.DbTypeConvert(dbType));
                //else
                //    return ((SqlParameterCollection)sqlCommand.Parameters).Add(parameterName, this.DbTypeConvert(dbType), size);

                return null;
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        System.Data.SqlDbType DbTypeConvert(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.BigInt:
                    return System.Data.SqlDbType.BigInt;

                case DbType.Binary:
                    return System.Data.SqlDbType.Binary;

                case DbType.Bit:
                    return System.Data.SqlDbType.Bit;

                case DbType.Char:
                    return System.Data.SqlDbType.Char;

                case DbType.Date:
                    return System.Data.SqlDbType.Date;

                case DbType.DateTime:
                    return System.Data.SqlDbType.DateTime;

                case DbType.DateTime2:
                    return System.Data.SqlDbType.DateTime2;

                case DbType.DateTimeOffset:
                    return System.Data.SqlDbType.DateTimeOffset;

                case DbType.Decimal:
                    return System.Data.SqlDbType.Decimal;

                case DbType.Float:
                    return System.Data.SqlDbType.Float;

                case DbType.Image:
                    return System.Data.SqlDbType.Image;

                case DbType.Int:
                    return System.Data.SqlDbType.Int;

                case DbType.Money:
                    return System.Data.SqlDbType.Money;

                case DbType.NChar:
                    return System.Data.SqlDbType.NChar;

                case DbType.NText:
                    return System.Data.SqlDbType.NText;

                case DbType.NVarChar:
                    return System.Data.SqlDbType.NVarChar;

                case DbType.Real:
                    return System.Data.SqlDbType.Real;

                case DbType.SmallDateTime:
                    return System.Data.SqlDbType.SmallDateTime;

                case DbType.SmallInt:
                    return System.Data.SqlDbType.SmallInt;

                case DbType.SmallMoney:
                    return System.Data.SqlDbType.SmallMoney;

                case DbType.Structured:
                    return System.Data.SqlDbType.Structured;

                case DbType.Text:
                    return System.Data.SqlDbType.Text;

                case DbType.Time:
                    return System.Data.SqlDbType.Time;

                case DbType.Timestamp:
                    return System.Data.SqlDbType.Timestamp;

                case DbType.TinyInt:
                    return System.Data.SqlDbType.TinyInt;

                case DbType.Udt:
                    return System.Data.SqlDbType.Udt;

                case DbType.UniqueIdentifier:
                    return System.Data.SqlDbType.UniqueIdentifier;

                case DbType.VarBinary:
                    return System.Data.SqlDbType.VarBinary;

                case DbType.VarChar:
                    return System.Data.SqlDbType.VarChar;

                case DbType.Variant:
                    return System.Data.SqlDbType.Variant;

                case DbType.Xml:
                    return System.Data.SqlDbType.Xml;

                default:
                    return System.Data.SqlDbType.Variant;
            }
        }

        DbCommand IDatabase.Command
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand;
            }
        }

        DbConnection IDatabase.Connection
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Connection;
            }
        }

        DbDataAdapter IDatabase.DataAdapter
        {
            get
            {
                return this.sqlDataAdapter;
            }
        }

        DbTransaction IDatabase.Transaction
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Transaction;
            }
        }

        void IDatabase.DeriveParameters()
        {
            throw new NotImplementedException();
        }

        void IDatabase.Close()
        {
            try
            {
                if (this.sqlDataAdapter.SelectCommand.Connection != null)
                {
                    this.sqlDataAdapter.SelectCommand.Connection.Close();
                    this.sqlDataAdapter.SelectCommand.Connection.Dispose();
                }

                if (this.sqlDataAdapter.SelectCommand != null)
                {
                    this.sqlDataAdapter.SelectCommand.Dispose();
                }

                if (this.sqlDataAdapter != null)
                {
                    this.sqlDataAdapter.Dispose();
                }
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }
    }
}