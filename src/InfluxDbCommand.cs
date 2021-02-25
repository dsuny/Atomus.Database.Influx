using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.UI.WebControls.WebParts;

namespace Atomus.Database
{

    public class InfluxDbCommand : DbCommand, ICore
    {
        private readonly SqlCommand sqlCommand = new SqlCommand();

        private DbConnection dbConnection;
        private int Timeout;

        public override string CommandText { get; set; }
        public override int CommandTimeout
        {
            get
            {
                return this.Timeout;
                //return (int)((InfluxDbConnection)this.DbConnection).InfluxDbClient.RequestClient.Configuration.HttpClient.Timeout.TotalMilliseconds;
            }
            set
            {
                this.Timeout = value;
                //((InfluxDbConnection)this.DbConnection).InfluxDbClient.RequestClient.Configuration.HttpClient.Timeout = new TimeSpan(value);
            }
        }
        public override CommandType CommandType
        {
            get
            {
                return this.sqlCommand.CommandType;
                //return CommandType.Text;
            }
            set
            {
                //this.sqlCommand.CommandType = value;
            }
        }
        public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        protected override DbConnection DbConnection
        {
            get
            {
                return this.dbConnection;
            }
            set
            {
                this.dbConnection = value;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return this.sqlCommand.Parameters;
                //return null;
            }
        }

        protected override DbTransaction DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Cancel()
        {
            throw new NotImplementedException();
        }


        public override int ExecuteNonQuery()
        {
            throw new NotImplementedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            return this.sqlCommand.CreateParameter();
            //return null;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }
    }
}
