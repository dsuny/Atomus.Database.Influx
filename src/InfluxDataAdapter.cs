using InfluxData.Net.InfluxDb.Models;
using InfluxData.Net.InfluxDb.Models.Responses;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Atomus.Database
{
    public class InfluxDataAdapter : DbDataAdapter, ICore
    {
        //
        // 요약:
        //     추가 하거나 행을 새로 고칩니다는 System.Data.DataSet합니다.
        //
        // 매개 변수:
        //   dataSet:
        //     A System.Data.DataSet 레코드와 함께 입력 하 고 필요한 경우 스키마입니다.
        //
        // 반환 값:
        //     성공적으로 추가 되거나 새로 고쳐진 행의 수는 System.Data.DataSet합니다. 이 행을 반환 하지 않는 문에 의해 영향을 받는
        //     행을 포함 되지 않습니다.
        public override int Fill(DataSet dataSet)
        {
            this.SelectCommand.Connection.Open();

            if (this.SelectCommand.CommandType == CommandType.Text)
                return this.FillCommandTypeText(dataSet);

            if (this.SelectCommand.CommandType == CommandType.StoredProcedure)
                return this.FillCommandTypeStoredProcedure(dataSet);

            return 0;
        }


        public IEnumerable<Serie> FillCommandTest()
        {
            this.SelectCommand.Connection.Open();

            return ((InfluxDbConnection)this.SelectCommand.Connection).InfluxDbClient.Client.QueryAsync(this.SelectCommand.CommandText, this.SelectCommand.Connection.Database).Result;
        }

        public int FillCommandTypeStoredTest(IEnumerable<Serie> series)
        {
            DateTime dateTime = DateTime.MinValue;
            Dictionary<string, object> Fields = new Dictionary<string, object>();
            List<Point> points = new List<Point>();
            int index;
            string columnName;

            foreach (Serie _serie in series)
            {
                index = 0;
                for (int iR = 0; iR < _serie.Values.Count; iR++)
                {
                    Fields = new Dictionary<string, object>();

                    for (int i = 0; i < _serie.Columns.Count; i++)
                    {
                        columnName = _serie.Columns[i];

                        if (columnName.StartsWith("sum_"))
                            columnName = columnName.Replace("sum_", "");

                        if (columnName == "time")
                            dateTime = (DateTime)_serie.Values[index][i];
                        else
                            try
                            {
                                if (_serie.Values[index][i] is int)
                                    Fields.Add(columnName, (float)(int)_serie.Values[index][i]);

                                else if (_serie.Values[index][i] is float)
                                    Fields.Add(columnName, (float)_serie.Values[index][i]);

                                else if (_serie.Values[index][i] is double)
                                    Fields.Add(columnName, (float)(double)_serie.Values[index][i]);

                                else if (_serie.Values[index][i] is long)
                                    Fields.Add(columnName, (float)(long)_serie.Values[index][i]);

                                else if (_serie.Values[index][i] == null)
                                    ;
                                else
                                    Fields.Add(columnName, _serie.Values[index][i]);
                            }
                            catch (Exception)
                            {
                                Fields.Add(columnName, _serie.Values[index][i]);
                            }
                    }

                    index += 1;

                    points.Add(new Point()
                    {
                        Name = this.SelectCommand.CommandText, //"EDGE_TAG_HISTORY_TS_DSUN", // serie/measurement/table to write into
                        Tags = new Dictionary<string, object>(),
                        Fields = Fields,
                        Timestamp = dateTime
                    });
                }
            }

            this.SelectCommand.Connection.Open();

            var response = ((InfluxDbConnection)this.SelectCommand.Connection).InfluxDbClient.Client.WriteAsync(points, this.SelectCommand.Connection.Database).Result;

            return 1;
        }



        public int FillCommandTypeText(DataSet dataSet)
        {
            DataTable dataTable;
            DataRow dataRow;
            int cnt;

            try
            {
                //((InfluxDbCommand)this.SelectCommand).Connection.State = ConnectionState.Executing;
                //((InfluxDbConnection)this.SelectCommand.Connection).InfluxDbClient.RequestClient.Configuration.HttpClient.Timeout = new TimeSpan(this.SelectCommand.CommandTimeout);
                var response = ((InfluxDbConnection)this.SelectCommand.Connection).InfluxDbClient.Client.QueryAsync(this.SelectCommand.CommandText, this.SelectCommand.Connection.Database).Result;

                cnt = 0;

                foreach (Serie _serie in response)
                {
                    dataTable = new DataTable(_serie.Name);

                    for (int iC = 0; iC < _serie.Columns.Count; iC++)
                    {
                        if (_serie.Values.Count > 0)
                        {
                            try
                            {
                                var ab = (from aa in _serie.Values
                                              //where aa[iC] != null
                                          select aa[iC]).Max();

                                dataTable.Columns.Add(_serie.Columns[iC], ab.GetType());
                            }
                            catch (Exception)
                            {
                                dataTable.Columns.Add(_serie.Columns[iC], typeof(decimal));
                            }
                            //if (ab != null)
                            //    dataTable.Columns.Add(_serie.Columns[iC], ab.GetType());
                            //else
                            //    dataTable.Columns.Add(_serie.Columns[iC]);
                        }
                        else
                            dataTable.Columns.Add(_serie.Columns[iC]);
                    }

                    for (int iR = 0; iR < _serie.Values.Count; iR++)
                    {
                        dataRow = dataTable.Rows.Add();

                        for (int iC = 0; iC < _serie.Columns.Count; iC++)
                        {
                            if (_serie.Values[iR][iC] == null)
                                dataRow[iC] = DBNull.Value;
                            else
                                dataRow[iC] = _serie.Values[iR][iC];
                        }

                        cnt += 1;
                    }

                    dataSet.Tables.Add(dataTable);

                    //dataTable = new DataTable(_serie.Name + "_Tags");
                    //dataTable.Columns.Add("Key");
                    //dataTable.Columns.Add("Value");

                    //for (int iC = 0; iC < _serie.Tags.Count; iC++)
                    //{
                    //    dataRow = dataTable.Rows.Add();
                    //    dataRow["Key"] = _serie.Tags.Keys.ToList()[iC];
                    //    dataRow["Value"] = _serie.Tags.ToList()[iC];
                    //    cnt += 1;
                    //}

                    //dataSet.Tables.Add(dataTable);
                }
            }
            catch (Exception)
            {

                throw;
            }

            return cnt;
        }

        public int FillCommandTypeStoredProcedure(DataSet dataSet)
        {
            DateTime dateTime = DateTime.MinValue;
            Dictionary<string, object> Fields = new Dictionary<string, object>();

            foreach (DbParameter dbParameter in this.SelectCommand.Parameters)
            {
                if (dbParameter.ParameterName == "Timestamp")
                    dateTime = (DateTime)dbParameter.Value;
                else
                    Fields.Add(dbParameter.ParameterName, dbParameter.Value);
            }

            var pointToWrite = new Point()
            {
                Name = this.SelectCommand.CommandText, //"EDGE_TAG_HISTORY_TS_DSUN", // serie/measurement/table to write into
                Tags = new Dictionary<string, object>(),
                Fields = Fields,
                Timestamp = (dateTime == DateTime.MinValue) ?
                            ((this.GetAttribute("Point.Timestamp") == null || this.GetAttribute("Point.Timestamp") == "DateTime.Now") ? DateTime.Now : DateTime.UtcNow)
                            : dateTime // optional (can be set to any DateTime moment)
            };

            var response = ((InfluxDbConnection)this.SelectCommand.Connection).InfluxDbClient.Client.WriteAsync(pointToWrite, this.SelectCommand.Connection.Database).Result;

            return 1;
        }
    }

}
