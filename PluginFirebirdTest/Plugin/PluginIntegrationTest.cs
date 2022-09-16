using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginFirebird.DataContracts;
using PluginFirebird.Helper;
using Xunit;
using Record = Naveego.Sdk.Plugins.Record;

namespace PluginMySQLTest.Plugin
{
    public class PluginIntegrationTest
    {
        private Settings GetSettings()
        {
            return new Settings
            {
                Hostname = "",
                Port = "",
                Database = "",
                Username = "",
                Password = ""
            };
        }

        private ConnectRequest GetConnectSettings()
        {
            var settings = GetSettings();

            return new ConnectRequest
            {
                SettingsJson = JsonConvert.SerializeObject(settings),
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
        }

        private Schema GetTestSchema(string id = "test", string name = "test", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "Id",
                        Name = "Id",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "Name",
                        Name = "Name",
                        Type = PropertyType.String,
                        TypeAtSource = "VARCHAR(300)"
                    }
                }
            };
        }
        
        private Schema GetTestReplicationSchema(string id = "test", string name = "test", string query = "")
        {
            // --- Note: Changed to fit the schema of the PERSONS table ---
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "LASTNAME",
                        Name = "LASTNAME",
                        Type = PropertyType.String,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "FIRSTNAME",
                        Name = "FIRSTNAME",
                        Type = PropertyType.String,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "ADDRESS",
                        Name = "ADDRESS",
                        Type = PropertyType.String,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "CITY",
                        Name = "CITY",
                        Type = PropertyType.String,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "PERSONID",
                        Name = "PERSONID",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "DateTime",
                        Name = "DateTime",
                        Type = PropertyType.Datetime,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "Date",
                        Name = "Date",
                        Type = PropertyType.Date,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "Time",
                        Name = "Time",
                        Type = PropertyType.Time,
                        IsKey = false
                    },
                    new Property
                    {
                        Id = "Decimal",
                        Name = "Decimal",
                        Type = PropertyType.Decimal,
                        TypeAtSource = "DECIMAL(10,2)",
                        IsKey = false
                    }
                }
            };
        }

        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);
            Assert.Equal("", response.SettingsError);
            Assert.Equal("", response.ConnectionError);
            Assert.Equal("", response.OauthError);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
                SampleSize = 10
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Equal(28, response.Schemas.Count);

            var schema = response.Schemas.FirstOrDefault(s => s.Name == "PERSONS");
            Assert.Equal($"\"PERSONS\"", schema.Id);
            Assert.Equal("PERSONS", schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(5, schema.Properties.Count);

            var property = schema.Properties[4];
            Assert.Equal("\"PERSONID\"", property.Id);
            Assert.Equal("PERSONID", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTableTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("\"PERSONS\"", "PERSONS")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal($"\"PERSONS\"", schema.Id);
            Assert.Equal("PERSONS", schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(5, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal("\"LASTNAME\"", property.Id);
            Assert.Equal("LASTNAME", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("test", "test", $"SELECT * FROM \"PERSONS\"")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal($"test", schema.Id);
            Assert.Equal("test", schema.Name);
            Assert.Equal($"SELECT * FROM \"PERSONS\"", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(5, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal("\"LASTNAME\"", property.Id);
            Assert.Equal("LASTNAME", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryBadSyntaxTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("test", "test", $"bad syntax")}
            };

            // act
            client.Connect(connectRequest);

            try
            {
                //var response = client.DiscoverSchemas(request);
                client.DiscoverSchemas(request);
            }
            catch (Exception e)
            {
                // assert
                Assert.IsType<RpcException>(e);
                Assert.Contains("Status(StatusCode=\"Unknown\", Detail=\"Dynamic SQL Error", e.Message);
                Assert.Contains("SQL error code = -104", e.Message);
                Assert.Contains("Token unknown - line 1, column 1\nbad", e.Message);
            }

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }


        [Fact]
        public async Task ReadStreamTableSchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            //var schema = GetTestSchema("`classicmodels`.`customers`", "classicmodels.customers");
            var schema = GetTestSchema("\"PERSONS\"", "PERSONS");
            
            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(22, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal("Bell", record["\"LASTNAME\""]);
            Assert.Equal("Kiara", record["\"FIRSTNAME\""]);
            Assert.Equal("7553 Beech Drive", record["\"ADDRESS\""]);
            Assert.Equal("Warner, NH", record["\"CITY\""]);
            Assert.Equal((long) 1, record["\"PERSONID\""]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamQuerySchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("test", "test", $"SELECT * FROM \"PERSONS\"");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(22, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal((long) 1, record["\"PERSONID\""]);
            Assert.Equal("Warner, NH", record["\"CITY\""]);
            Assert.Equal("7553 Beech Drive", record["\"ADDRESS\""]);
            Assert.Equal("Bell", record["\"LASTNAME\""]);
            Assert.Equal("Kiara", record["\"FIRSTNAME\""]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("\"PERSONS\"", "PERSONS");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
                Limit = 10
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(10, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest()
            {
                Schema = GetTestSchema(),
                CommitSlaSeconds = 1,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        GoldenTableName = "gr_test",
                        VersionTableName = "vr_test"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 2
                }
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReplicationWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var prepareWriteRequest = new PrepareWriteRequest()
            {
                Schema = GetTestReplicationSchema(),
                CommitSlaSeconds = 1000,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        GoldenTableName = "gr_Persons",
                        VersionTableName = "vr_Persons"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };

            var records = new List<Record>()
            {
                {
                    new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        CorrelationId = "Persons",
                        RecordId = "record1",
                        DataJson = @$"{{
    ""Lastname"":'Reynolds',
    ""Firstname"":'Laura',
    ""ADDRESS"":'921 Cone Street',
    ""CITY"":'Petosky, MI',
    ""PERSONID"":4,
    ""DateTime"":'{DateTime.Now:MM/dd/yyyy HH:mm:ss}',
    ""Date"":'{DateTime.Now.Date:MM/dd/yyyy}',
    ""Time"":'{DateTime.Now:HH:mm:ss}',
    ""Decimal"":13.04
}}".Replace("\n", "").Replace("    ", ""),
                        Versions =
                        {
                            new RecordVersion
                            {
                                RecordId = "version1",
                                DataJson = @$"{{
    ""Lastname"":'Reynolds',
    ""Firstname"":'Laura',
    ""ADDRESS"":'921 Cone Street',
    ""CITY"":'Petosky, MI',
    ""PERSONID"":4,
    ""DateTime"":'{DateTime.Now:MM/dd/yyyy HH:mm:ss}',
    ""Date"":'{DateTime.Now.Date:MM/dd/yyyy}',
    ""Time"":'{DateTime.Now:HH:mm:ss}',
    ""Decimal"":13.04
}}".Replace("\n", "").Replace("    ", "")
                            }
                        }
                    }
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("Persons", recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task WriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFirebird.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var configureRequest = new ConfigureWriteRequest
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson = JsonConvert.SerializeObject(new ConfigureWriteFormData
                    {
                        StoredProcedure = "\"UpsertIntoPersons\""
                    })
                }
            };

            var records = new List<Record>()
            {
                {
                    new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        CorrelationId = "test",
                        RecordId = "record1",
                        DataJson = "{\"LASTNAME\":\"Kelly\",\"FIRSTNAME\":\"James\",\"ADDRESS\":\"937 Pine Drive\",\"CITY\":\"Paradise, HW\",\"PERSONID\":20}",
                    }
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);

            var configureResponse = client.ConfigureWrite(configureRequest);

            var prepareWriteRequest = new PrepareWriteRequest()
            {
                Schema = configureResponse.Schema,
                CommitSlaSeconds = 1000,
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}