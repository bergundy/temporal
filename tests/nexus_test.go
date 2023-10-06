package tests

// func (s *clientIntegrationSuite) TestNexusStartOperation() {
// 	client, err := nexus.NewClient(nexus.ClientOptions{
// 		ServiceBaseURL: fmt.Sprintf("http://%s/my/service", s.nexusHTTPAddress),
// 	})
// 	s.Require().NoError(err)
// 	startOptions, err := nexus.NewStartOperationOptions("foo", SomeJSONStruct{SomeField: "value"})
// 	s.Require().NoError(err)
// 	startOptions.CallbackURL = "http://some-callback-url"
// 	startOptions.RequestID = "abcd"

// 	result, err := client.StartOperation(NewContext(), startOptions)
// 	s.Require().NoError(err)
// 	s.Require().NotNil(result.Successful)
// 	response := result.Successful
// 	defer response.Body.Close()
// 	body, err := io.ReadAll(response.Body)
// 	s.Require().NoError(err)
// 	s.Require().Equal(response.Header.Get("Content-Type"), "application/json")
// 	s.Require().Equal("\"ok\"", string(body))
// }
