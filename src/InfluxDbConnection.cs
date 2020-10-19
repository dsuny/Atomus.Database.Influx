using InfluxData.Net.Common.Enums;
using InfluxData.Net.InfluxDb;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Atomus.Database
{

    public class InfluxDbConnection : DbConnection, ICore
    {
        internal InfluxDbClient InfluxDbClient { get; set; }
        private string connectionString;
        private string database;
        private ConnectionState connectionState = ConnectionState.Closed;

        internal InfluxDbVersion InfluxDbVersion;
        internal string EndpointUri { get; set; }
        internal string UserID { get; set; }
        internal string Password { get; set; }
        internal DbCommand dbCommand;

        //MS-SQL : Data Source=atomus.dsun.kr,4004;Initial Catalog=DBNAME;Persist Security Info=True;User ID=;Password=
        //Influx : Data Source=http://atomus.dsun.kr:4004;Initial Catalog=DBNAME;Influx Db Version=Latest;User ID=;Password=
        public override string ConnectionString
        {
            get
            {
                return this.connectionString;
            }
            set
            {
                this.connectionString = value;

                var cs = (from a in this.connectionString.Split(';')
                          where a.Contains("Data Source")
                          select a);

                if (cs != null && cs.Count() == 1)
                {
                    var ic = cs.ToList()[0].Split('=');

                    if (ic != null && ic.Length == 2)
                        this.EndpointUri = ic[1].Trim();
                }


                cs = (from a in this.connectionString.Split(';')
                      where a.Contains("Initial Catalog")
                      select a);

                if (cs != null && cs.Count() == 1)
                {
                    var ic = cs.ToList()[0].Split('=');

                    if (ic != null && ic.Length == 2)
                        this.database = ic[1].Trim();
                }


                cs = (from a in this.connectionString.Split(';')
                      where a.Contains("Influx Db Version")
                      select a);

                if (cs != null && cs.Count() == 1)
                {
                    var ic = cs.ToList()[0].Split('=');

                    if (ic != null && ic.Length == 2)
                        this.InfluxDbVersion = (InfluxDbVersion)Enum.Parse(typeof(InfluxDbVersion), ic[1].Trim());
                }

                cs = (from a in this.connectionString.Split(';')
                      where a.Contains("User ID")
                      select a);

                if (cs != null && cs.Count() == 1)
                {
                    var ic = cs.ToList()[0].Split('=');

                    if (ic != null && ic.Length == 2)
                        this.UserID = ic[1].Trim();
                }

                cs = (from a in this.connectionString.Split(';')
                      where a.Contains("Password")
                      select a);

                if (cs != null && cs.Count() == 1)
                {
                    var ic = cs.ToList()[0].Split('=');

                    if (ic != null && ic.Length == 2)
                        this.Password = ic[1].Trim();
                }
            }
        }

        public override string Database => this.database;
        public override string DataSource => this.database;
        public override string ServerVersion => this.InfluxDbVersion.ToString();

        public override ConnectionState State => this.connectionState;

        public override void ChangeDatabase(string databaseName)
        {
            this.database = databaseName;
        }

        public override void Close()
        {
            this.InfluxDbClient.RequestClient.Configuration.HttpClient.Dispose();
            this.connectionState = ConnectionState.Closed;
        }

        public override void Open()
        {
            this.CreateDbCommand();
        }
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            if (this.InfluxDbClient == null)
                try
                {
                    this.connectionState = ConnectionState.Connecting;
                    this.InfluxDbClient = new InfluxDbClient(this.EndpointUri, this.UserID, this.Password, this.InfluxDbVersion);
                    this.InfluxDbClient.RequestClient.Configuration.HttpClient.Timeout = new TimeSpan(this.dbCommand.CommandTimeout * 10000);
                    this.connectionState = ConnectionState.Open;
                }
                catch (Exception)
                {
                    this.connectionState = ConnectionState.Closed;
                    throw;
                }

            return this.dbCommand;
        }
    }
}
