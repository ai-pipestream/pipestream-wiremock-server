# GetBlob Proto Verification

## ✅ GetBlob EXISTS in Proto

**Proto File**: `pipestream-protos/repo/proto/ai/pipestream/repository/pipedoc/v1/pipedoc_service.proto`

**Service Definition** (Line 187):
```protobuf
service PipeDocService {
  // ... other RPCs ...
  
  // Retrieve blob binary content using a FileStorageReference.
  // This is used for Level 2 hydration when modules need raw binary data (e.g., parsers).
  // The FileStorageReference is typically obtained from a PipeDoc's BlobBag after Level 1 hydration.
  rpc GetBlob(GetBlobRequest) returns (GetBlobResponse);
}
```

**Request Message** (Lines 123-126):
```protobuf
message GetBlobRequest {
  // Storage reference containing drive name, object key, and optional version ID
  ai.pipestream.data.v1.FileStorageReference storage_ref = 1;
}
```

**Response Message** (Lines 129-134):
```protobuf
message GetBlobResponse {
  // The raw binary content of the blob
  bytes data = 1;
  // Size of the blob in bytes
  int64 size_bytes = 2;
  // MIME type of the blob content (if available)
  string content_type = 3;
}
```

## ✅ Generated Classes Verified

After running `./gradlew clean generateProtos`, the following classes are generated:

- `build/generated/source/proto/main/java/ai/pipestream/repository/pipedoc/v1/GetBlobRequest.java`
- `build/generated/source/proto/main/java/ai/pipestream/repository/pipedoc/v1/GetBlobResponse.java`
- `build/generated/source/proto/main/java/ai/pipestream/repository/pipedoc/v1/GetBlobRequestOrBuilder.java`
- `build/generated/source/proto/main/java/ai/pipestream/repository/pipedoc/v1/GetBlobResponseOrBuilder.java`

## ✅ Compilation Verified

Both `compileJava` and `compileTestJava` succeed with GetBlob support.

## 🔧 If GetBlob Classes Are Missing

If the other coder is seeing "GetBlob doesn't exist", they need to:

1. **Regenerate protos**:
   ```bash
   cd /Users/krickert/IdeaProjects/pipestream-wiremock-server
   ./gradlew clean generateProtos
   ```

2. **Verify proto fetch**:
   - The proto toolchain fetches from: `https://github.com/ai-pipestream/pipestream-protos.git`
   - Branch: `main`
   - The proto file is at: `repo/proto/ai/pipestream/repository/pipedoc/v1/pipedoc_service.proto`

3. **Check generated classes**:
   ```bash
   find build/generated/source/proto/main/java -name "*GetBlob*"
   ```

4. **Verify imports in code**:
   ```java
   import ai.pipestream.repository.pipedoc.v1.GetBlobRequest;
   import ai.pipestream.repository.pipedoc.v1.GetBlobResponse;
   ```

## 📝 Implementation Status

**PipeDocServiceMock.java**: ✅ GetBlob methods implemented
- `registerBlob()` - Register blob data
- `mockGetBlob()` - Internal stub creation
- `mockGetBlobReturns()` - Mock successful response
- `mockGetBlobNotFound()` - Mock NOT_FOUND
- `mockGetBlobUnavailable()` - Mock UNAVAILABLE
- `mockGetBlobError()` - Mock INTERNAL error

**PipeDocServiceMockTest.java**: ✅ GetBlob tests implemented
- 12+ test cases covering all scenarios

## ✅ Verification Complete

GetBlob is **definitely** in the proto and the implementation is complete. The issue is likely that protos need to be regenerated in the wiremock-server project.


