import type * as gax from 'google-gax';
import type { Callback, CallOptions, Descriptors, ClientOptions, LROperation, IamClient, IamProtos, LocationsClient, LocationProtos } from 'google-gax';
import * as protos from '../../protos/protos';
/**
 *  Provides interfaces for using Cloud KMS Autokey to provision new
 *  {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys}, ready for Customer Managed
 *  Encryption Key (CMEK) use, on-demand. To support certain client tooling, this
 *  feature is modeled around a {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle}
 *  resource: creating a {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle} in a resource
 *  project and given location triggers Cloud KMS Autokey to provision a
 *  {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} in the configured key project and
 *  the same location.
 *
 *  Prior to use in a given resource project,
 *  {@link protos.google.cloud.kms.v1.AutokeyAdmin.UpdateAutokeyConfig|UpdateAutokeyConfig}
 *  should have been called on an ancestor folder, setting the key project where
 *  Cloud KMS Autokey should create new
 *  {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys}. See documentation for additional
 *  prerequisites. To check what key project, if any, is currently configured on
 *  a resource project's ancestor folder, see
 *  {@link protos.google.cloud.kms.v1.AutokeyAdmin.ShowEffectiveAutokeyConfig|ShowEffectiveAutokeyConfig}.
 * @class
 * @memberof v1
 */
export declare class AutokeyClient {
    private _terminated;
    private _opts;
    private _providedCustomServicePath;
    private _gaxModule;
    private _gaxGrpc;
    private _protos;
    private _defaults;
    private _universeDomain;
    private _servicePath;
    auth: gax.GoogleAuth;
    descriptors: Descriptors;
    warn: (code: string, message: string, warnType?: string) => void;
    innerApiCalls: {
        [name: string]: Function;
    };
    iamClient: IamClient;
    locationsClient: LocationsClient;
    pathTemplates: {
        [name: string]: gax.PathTemplate;
    };
    operationsClient: gax.OperationsClient;
    autokeyStub?: Promise<{
        [name: string]: Function;
    }>;
    /**
     * Construct an instance of AutokeyClient.
     *
     * @param {object} [options] - The configuration object.
     * The options accepted by the constructor are described in detail
     * in [this document](https://github.com/googleapis/gax-nodejs/blob/main/client-libraries.md#creating-the-client-instance).
     * The common options are:
     * @param {object} [options.credentials] - Credentials object.
     * @param {string} [options.credentials.client_email]
     * @param {string} [options.credentials.private_key]
     * @param {string} [options.email] - Account email address. Required when
     *     using a .pem or .p12 keyFilename.
     * @param {string} [options.keyFilename] - Full path to the a .json, .pem, or
     *     .p12 key downloaded from the Google Developers Console. If you provide
     *     a path to a JSON file, the projectId option below is not necessary.
     *     NOTE: .pem and .p12 require you to specify options.email as well.
     * @param {number} [options.port] - The port on which to connect to
     *     the remote host.
     * @param {string} [options.projectId] - The project ID from the Google
     *     Developer's Console, e.g. 'grape-spaceship-123'. We will also check
     *     the environment variable GCLOUD_PROJECT for your project ID. If your
     *     app is running in an environment which supports
     *     {@link https://developers.google.com/identity/protocols/application-default-credentials Application Default Credentials},
     *     your project ID will be detected automatically.
     * @param {string} [options.apiEndpoint] - The domain name of the
     *     API remote host.
     * @param {gax.ClientConfig} [options.clientConfig] - Client configuration override.
     *     Follows the structure of {@link gapicConfig}.
     * @param {boolean} [options.fallback] - Use HTTP/1.1 REST mode.
     *     For more information, please check the
     *     {@link https://github.com/googleapis/gax-nodejs/blob/main/client-libraries.md#http11-rest-api-mode documentation}.
     * @param {gax} [gaxInstance]: loaded instance of `google-gax`. Useful if you
     *     need to avoid loading the default gRPC version and want to use the fallback
     *     HTTP implementation. Load only fallback version and pass it to the constructor:
     *     ```
     *     const gax = require('google-gax/build/src/fallback'); // avoids loading google-gax with gRPC
     *     const client = new AutokeyClient({fallback: true}, gax);
     *     ```
     */
    constructor(opts?: ClientOptions, gaxInstance?: typeof gax | typeof gax.fallback);
    /**
     * Initialize the client.
     * Performs asynchronous operations (such as authentication) and prepares the client.
     * This function will be called automatically when any class method is called for the
     * first time, but if you need to initialize it before calling an actual method,
     * feel free to call initialize() directly.
     *
     * You can await on this method if you want to make sure the client is initialized.
     *
     * @returns {Promise} A promise that resolves to an authenticated service stub.
     */
    initialize(): Promise<{
        [name: string]: Function;
    }>;
    /**
     * The DNS address for this API service.
     * @deprecated Use the apiEndpoint method of the client instance.
     * @returns {string} The DNS address for this service.
     */
    static get servicePath(): string;
    /**
     * The DNS address for this API service - same as servicePath.
     * @deprecated Use the apiEndpoint method of the client instance.
     * @returns {string} The DNS address for this service.
     */
    static get apiEndpoint(): string;
    /**
     * The DNS address for this API service.
     * @returns {string} The DNS address for this service.
     */
    get apiEndpoint(): string;
    get universeDomain(): string;
    /**
     * The port for this API service.
     * @returns {number} The default port for this service.
     */
    static get port(): number;
    /**
     * The scopes needed to make gRPC calls for every method defined
     * in this service.
     * @returns {string[]} List of default scopes.
     */
    static get scopes(): string[];
    getProjectId(): Promise<string>;
    getProjectId(callback: Callback<string, undefined, undefined>): void;
    /**
     * Returns the {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. Name of the {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle} resource,
     *   e.g.
     *   `projects/{PROJECT_ID}/locations/{LOCATION}/keyHandles/{KEY_HANDLE_ID}`.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/autokey.get_key_handle.js</caption>
     * region_tag:cloudkms_v1_generated_Autokey_GetKeyHandle_async
     */
    getKeyHandle(request?: protos.google.cloud.kms.v1.IGetKeyHandleRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IKeyHandle,
        protos.google.cloud.kms.v1.IGetKeyHandleRequest | undefined,
        {} | undefined
    ]>;
    getKeyHandle(request: protos.google.cloud.kms.v1.IGetKeyHandleRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IKeyHandle, protos.google.cloud.kms.v1.IGetKeyHandleRequest | null | undefined, {} | null | undefined>): void;
    getKeyHandle(request: protos.google.cloud.kms.v1.IGetKeyHandleRequest, callback: Callback<protos.google.cloud.kms.v1.IKeyHandle, protos.google.cloud.kms.v1.IGetKeyHandleRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Lists {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandles}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. Name of the resource project and location from which to list
     *   {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandles}, e.g.
     *   `projects/{PROJECT_ID}/locations/{LOCATION}`.
     * @param {string} [request.filter]
     *   Optional. Filter to apply when listing
     *   {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandles}, e.g.
     *   `resource_type_selector="{SERVICE}.googleapis.com/{TYPE}"`.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.ListKeyHandlesResponse|ListKeyHandlesResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/autokey.list_key_handles.js</caption>
     * region_tag:cloudkms_v1_generated_Autokey_ListKeyHandles_async
     */
    listKeyHandles(request?: protos.google.cloud.kms.v1.IListKeyHandlesRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IListKeyHandlesResponse,
        protos.google.cloud.kms.v1.IListKeyHandlesRequest | undefined,
        {} | undefined
    ]>;
    listKeyHandles(request: protos.google.cloud.kms.v1.IListKeyHandlesRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IListKeyHandlesResponse, protos.google.cloud.kms.v1.IListKeyHandlesRequest | null | undefined, {} | null | undefined>): void;
    listKeyHandles(request: protos.google.cloud.kms.v1.IListKeyHandlesRequest, callback: Callback<protos.google.cloud.kms.v1.IListKeyHandlesResponse, protos.google.cloud.kms.v1.IListKeyHandlesRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Creates a new {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle}, triggering the
     * provisioning of a new {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} for CMEK
     * use with the given resource type in the configured key project and the same
     * location. {@link protos.Operations.GetOperation|GetOperation} should be used to resolve
     * the resulting long-running operation and get the resulting
     * {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle} and
     * {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. Name of the resource project and location to create the
     *   {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle} in, e.g.
     *   `projects/{PROJECT_ID}/locations/{LOCATION}`.
     * @param {string} [request.keyHandleId]
     *   Optional. Id of the {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle}. Must be
     *   unique to the resource project and location. If not provided by the caller,
     *   a new UUID is used.
     * @param {google.cloud.kms.v1.KeyHandle} request.keyHandle
     *   Required. {@link protos.google.cloud.kms.v1.KeyHandle|KeyHandle} to create.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing
     *   a long running operation. Its `promise()` method returns a promise
     *   you can `await` for.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#long-running-operations | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/autokey.create_key_handle.js</caption>
     * region_tag:cloudkms_v1_generated_Autokey_CreateKeyHandle_async
     */
    createKeyHandle(request?: protos.google.cloud.kms.v1.ICreateKeyHandleRequest, options?: CallOptions): Promise<[
        LROperation<protos.google.cloud.kms.v1.IKeyHandle, protos.google.cloud.kms.v1.ICreateKeyHandleMetadata>,
        protos.google.longrunning.IOperation | undefined,
        {} | undefined
    ]>;
    createKeyHandle(request: protos.google.cloud.kms.v1.ICreateKeyHandleRequest, options: CallOptions, callback: Callback<LROperation<protos.google.cloud.kms.v1.IKeyHandle, protos.google.cloud.kms.v1.ICreateKeyHandleMetadata>, protos.google.longrunning.IOperation | null | undefined, {} | null | undefined>): void;
    createKeyHandle(request: protos.google.cloud.kms.v1.ICreateKeyHandleRequest, callback: Callback<LROperation<protos.google.cloud.kms.v1.IKeyHandle, protos.google.cloud.kms.v1.ICreateKeyHandleMetadata>, protos.google.longrunning.IOperation | null | undefined, {} | null | undefined>): void;
    /**
     * Check the status of the long running operation returned by `createKeyHandle()`.
     * @param {String} name
     *   The operation name that will be passed.
     * @returns {Promise} - The promise which resolves to an object.
     *   The decoded operation object has result and metadata field to get information from.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#long-running-operations | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/autokey.create_key_handle.js</caption>
     * region_tag:cloudkms_v1_generated_Autokey_CreateKeyHandle_async
     */
    checkCreateKeyHandleProgress(name: string): Promise<LROperation<protos.google.cloud.kms.v1.KeyHandle, protos.google.cloud.kms.v1.CreateKeyHandleMetadata>>;
    /**
     * Gets the access control policy for a resource. Returns an empty policy
     * if the resource exists and does not have a policy set.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.resource
     *   REQUIRED: The resource for which the policy is being requested.
     *   See the operation documentation for the appropriate value for this field.
     * @param {Object} [request.options]
     *   OPTIONAL: A `GetPolicyOptions` object for specifying options to
     *   `GetIamPolicy`. This field is only used by Cloud IAM.
     *
     *   This object should have the same structure as {@link google.iam.v1.GetPolicyOptions | GetPolicyOptions}.
     * @param {Object} [options]
     *   Optional parameters. You can override the default settings for this call, e.g, timeout,
     *   retries, paginations, etc. See {@link https://googleapis.github.io/gax-nodejs/interfaces/CallOptions.html | gax.CallOptions} for the details.
     * @param {function(?Error, ?Object)} [callback]
     *   The function which will be called with the result of the API call.
     *
     *   The second parameter to the callback is an object representing {@link google.iam.v1.Policy | Policy}.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link google.iam.v1.Policy | Policy}.
     *   The promise has a method named "cancel" which cancels the ongoing API call.
     */
    getIamPolicy(request: IamProtos.google.iam.v1.GetIamPolicyRequest, options?: gax.CallOptions | Callback<IamProtos.google.iam.v1.Policy, IamProtos.google.iam.v1.GetIamPolicyRequest | null | undefined, {} | null | undefined>, callback?: Callback<IamProtos.google.iam.v1.Policy, IamProtos.google.iam.v1.GetIamPolicyRequest | null | undefined, {} | null | undefined>): Promise<[IamProtos.google.iam.v1.Policy]>;
    /**
     * Returns permissions that a caller has on the specified resource. If the
     * resource does not exist, this will return an empty set of
     * permissions, not a NOT_FOUND error.
     *
     * Note: This operation is designed to be used for building
     * permission-aware UIs and command-line tools, not for authorization
     * checking. This operation may "fail open" without warning.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.resource
     *   REQUIRED: The resource for which the policy detail is being requested.
     *   See the operation documentation for the appropriate value for this field.
     * @param {string[]} request.permissions
     *   The set of permissions to check for the `resource`. Permissions with
     *   wildcards (such as '*' or 'storage.*') are not allowed. For more
     *   information see {@link https://cloud.google.com/iam/docs/overview#permissions | IAM Overview }.
     * @param {Object} [options]
     *   Optional parameters. You can override the default settings for this call, e.g, timeout,
     *   retries, paginations, etc. See {@link https://googleapis.github.io/gax-nodejs/interfaces/CallOptions.html | gax.CallOptions} for the details.
     * @param {function(?Error, ?Object)} [callback]
     *   The function which will be called with the result of the API call.
     *
     *   The second parameter to the callback is an object representing {@link google.iam.v1.TestIamPermissionsResponse | TestIamPermissionsResponse}.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link google.iam.v1.TestIamPermissionsResponse | TestIamPermissionsResponse}.
     *   The promise has a method named "cancel" which cancels the ongoing API call.
     */
    setIamPolicy(request: IamProtos.google.iam.v1.SetIamPolicyRequest, options?: gax.CallOptions | Callback<IamProtos.google.iam.v1.Policy, IamProtos.google.iam.v1.SetIamPolicyRequest | null | undefined, {} | null | undefined>, callback?: Callback<IamProtos.google.iam.v1.Policy, IamProtos.google.iam.v1.SetIamPolicyRequest | null | undefined, {} | null | undefined>): Promise<[IamProtos.google.iam.v1.Policy]>;
    /**
     * Returns permissions that a caller has on the specified resource. If the
     * resource does not exist, this will return an empty set of
     * permissions, not a NOT_FOUND error.
     *
     * Note: This operation is designed to be used for building
     * permission-aware UIs and command-line tools, not for authorization
     * checking. This operation may "fail open" without warning.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.resource
     *   REQUIRED: The resource for which the policy detail is being requested.
     *   See the operation documentation for the appropriate value for this field.
     * @param {string[]} request.permissions
     *   The set of permissions to check for the `resource`. Permissions with
     *   wildcards (such as '*' or 'storage.*') are not allowed. For more
     *   information see {@link https://cloud.google.com/iam/docs/overview#permissions | IAM Overview }.
     * @param {Object} [options]
     *   Optional parameters. You can override the default settings for this call, e.g, timeout,
     *   retries, paginations, etc. See {@link https://googleapis.github.io/gax-nodejs/interfaces/CallOptions.html | gax.CallOptions} for the details.
     * @param {function(?Error, ?Object)} [callback]
     *   The function which will be called with the result of the API call.
     *
     *   The second parameter to the callback is an object representing {@link google.iam.v1.TestIamPermissionsResponse | TestIamPermissionsResponse}.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link google.iam.v1.TestIamPermissionsResponse | TestIamPermissionsResponse}.
     *   The promise has a method named "cancel" which cancels the ongoing API call.
     *
     */
    testIamPermissions(request: IamProtos.google.iam.v1.TestIamPermissionsRequest, options?: gax.CallOptions | Callback<IamProtos.google.iam.v1.TestIamPermissionsResponse, IamProtos.google.iam.v1.TestIamPermissionsRequest | null | undefined, {} | null | undefined>, callback?: Callback<IamProtos.google.iam.v1.TestIamPermissionsResponse, IamProtos.google.iam.v1.TestIamPermissionsRequest | null | undefined, {} | null | undefined>): Promise<[IamProtos.google.iam.v1.TestIamPermissionsResponse]>;
    /**
     * Gets information about a location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Resource name for the location.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html | CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link google.cloud.location.Location | Location}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example
     * ```
     * const [response] = await client.getLocation(request);
     * ```
     */
    getLocation(request: LocationProtos.google.cloud.location.IGetLocationRequest, options?: gax.CallOptions | Callback<LocationProtos.google.cloud.location.ILocation, LocationProtos.google.cloud.location.IGetLocationRequest | null | undefined, {} | null | undefined>, callback?: Callback<LocationProtos.google.cloud.location.ILocation, LocationProtos.google.cloud.location.IGetLocationRequest | null | undefined, {} | null | undefined>): Promise<LocationProtos.google.cloud.location.ILocation>;
    /**
     * Lists information about the supported locations for this service. Returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   The resource that owns the locations collection, if applicable.
     * @param {string} request.filter
     *   The standard list filter.
     * @param {number} request.pageSize
     *   The standard list page size.
     * @param {string} request.pageToken
     *   The standard list page token.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Object}
     *   An iterable Object that allows {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | async iteration }.
     *   When you iterate the returned iterable, each element will be an object representing
     *   {@link google.cloud.location.Location | Location}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example
     * ```
     * const iterable = client.listLocationsAsync(request);
     * for await (const response of iterable) {
     *   // process response
     * }
     * ```
     */
    listLocationsAsync(request: LocationProtos.google.cloud.location.IListLocationsRequest, options?: CallOptions): AsyncIterable<LocationProtos.google.cloud.location.ILocation>;
    /**
     * Gets the latest state of a long-running operation.  Clients can use this
     * method to poll the operation result at intervals as recommended by the API
     * service.
     *
     * @param {Object} request - The request object that will be sent.
     * @param {string} request.name - The name of the operation resource.
     * @param {Object=} options
     *   Optional parameters. You can override the default settings for this call,
     *   e.g, timeout, retries, paginations, etc. See {@link
     *   https://googleapis.github.io/gax-nodejs/global.html#CallOptions | gax.CallOptions}
     *   for the details.
     * @param {function(?Error, ?Object)=} callback
     *   The function which will be called with the result of the API call.
     *
     *   The second parameter to the callback is an object representing
     *   {@link google.longrunning.Operation | google.longrunning.Operation}.
     * @return {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing
     * {@link google.longrunning.Operation | google.longrunning.Operation}.
     * The promise has a method named "cancel" which cancels the ongoing API call.
     *
     * @example
     * ```
     * const client = longrunning.operationsClient();
     * const name = '';
     * const [response] = await client.getOperation({name});
     * // doThingsWith(response)
     * ```
     */
    getOperation(request: protos.google.longrunning.GetOperationRequest, options?: gax.CallOptions | Callback<protos.google.longrunning.Operation, protos.google.longrunning.GetOperationRequest, {} | null | undefined>, callback?: Callback<protos.google.longrunning.Operation, protos.google.longrunning.GetOperationRequest, {} | null | undefined>): Promise<[protos.google.longrunning.Operation]>;
    /**
     * Lists operations that match the specified filter in the request. If the
     * server doesn't support this method, it returns `UNIMPLEMENTED`. Returns an iterable object.
     *
     * For-await-of syntax is used with the iterable to recursively get response element on-demand.
     *
     * @param {Object} request - The request object that will be sent.
     * @param {string} request.name - The name of the operation collection.
     * @param {string} request.filter - The standard list filter.
     * @param {number=} request.pageSize -
     *   The maximum number of resources contained in the underlying API
     *   response. If page streaming is performed per-resource, this
     *   parameter does not affect the return value. If page streaming is
     *   performed per-page, this determines the maximum number of
     *   resources in a page.
     * @param {Object=} options
     *   Optional parameters. You can override the default settings for this call,
     *   e.g, timeout, retries, paginations, etc. See {@link
     *   https://googleapis.github.io/gax-nodejs/global.html#CallOptions | gax.CallOptions} for the
     *   details.
     * @returns {Object}
     *   An iterable Object that conforms to {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | iteration protocols}.
     *
     * @example
     * ```
     * const client = longrunning.operationsClient();
     * for await (const response of client.listOperationsAsync(request));
     * // doThingsWith(response)
     * ```
     */
    listOperationsAsync(request: protos.google.longrunning.ListOperationsRequest, options?: gax.CallOptions): AsyncIterable<protos.google.longrunning.ListOperationsResponse>;
    /**
     * Starts asynchronous cancellation on a long-running operation.  The server
     * makes a best effort to cancel the operation, but success is not
     * guaranteed.  If the server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.  Clients can use
     * {@link Operations.GetOperation} or
     * other methods to check whether the cancellation succeeded or whether the
     * operation completed despite cancellation. On successful cancellation,
     * the operation is not deleted; instead, it becomes an operation with
     * an {@link Operation.error} value with a {@link google.rpc.Status.code} of
     * 1, corresponding to `Code.CANCELLED`.
     *
     * @param {Object} request - The request object that will be sent.
     * @param {string} request.name - The name of the operation resource to be cancelled.
     * @param {Object=} options
     *   Optional parameters. You can override the default settings for this call,
     * e.g, timeout, retries, paginations, etc. See {@link
     * https://googleapis.github.io/gax-nodejs/global.html#CallOptions | gax.CallOptions} for the
     * details.
     * @param {function(?Error)=} callback
     *   The function which will be called with the result of the API call.
     * @return {Promise} - The promise which resolves when API call finishes.
     *   The promise has a method named "cancel" which cancels the ongoing API
     * call.
     *
     * @example
     * ```
     * const client = longrunning.operationsClient();
     * await client.cancelOperation({name: ''});
     * ```
     */
    cancelOperation(request: protos.google.longrunning.CancelOperationRequest, options?: gax.CallOptions | Callback<protos.google.protobuf.Empty, protos.google.longrunning.CancelOperationRequest, {} | undefined | null>, callback?: Callback<protos.google.longrunning.CancelOperationRequest, protos.google.protobuf.Empty, {} | undefined | null>): Promise<protos.google.protobuf.Empty>;
    /**
     * Deletes a long-running operation. This method indicates that the client is
     * no longer interested in the operation result. It does not cancel the
     * operation. If the server doesn't support this method, it returns
     * `google.rpc.Code.UNIMPLEMENTED`.
     *
     * @param {Object} request - The request object that will be sent.
     * @param {string} request.name - The name of the operation resource to be deleted.
     * @param {Object=} options
     *   Optional parameters. You can override the default settings for this call,
     * e.g, timeout, retries, paginations, etc. See {@link
     * https://googleapis.github.io/gax-nodejs/global.html#CallOptions | gax.CallOptions}
     * for the details.
     * @param {function(?Error)=} callback
     *   The function which will be called with the result of the API call.
     * @return {Promise} - The promise which resolves when API call finishes.
     *   The promise has a method named "cancel" which cancels the ongoing API
     * call.
     *
     * @example
     * ```
     * const client = longrunning.operationsClient();
     * await client.deleteOperation({name: ''});
     * ```
     */
    deleteOperation(request: protos.google.longrunning.DeleteOperationRequest, options?: gax.CallOptions | Callback<protos.google.protobuf.Empty, protos.google.longrunning.DeleteOperationRequest, {} | null | undefined>, callback?: Callback<protos.google.protobuf.Empty, protos.google.longrunning.DeleteOperationRequest, {} | null | undefined>): Promise<protos.google.protobuf.Empty>;
    /**
     * Return a fully-qualified autokeyConfig resource name string.
     *
     * @param {string} folder
     * @returns {string} Resource name string.
     */
    autokeyConfigPath(folder: string): string;
    /**
     * Parse the folder from AutokeyConfig resource.
     *
     * @param {string} autokeyConfigName
     *   A fully-qualified path representing AutokeyConfig resource.
     * @returns {string} A string representing the folder.
     */
    matchFolderFromAutokeyConfigName(autokeyConfigName: string): string | number;
    /**
     * Return a fully-qualified cryptoKey resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_ring
     * @param {string} crypto_key
     * @returns {string} Resource name string.
     */
    cryptoKeyPath(project: string, location: string, keyRing: string, cryptoKey: string): string;
    /**
     * Parse the project from CryptoKey resource.
     *
     * @param {string} cryptoKeyName
     *   A fully-qualified path representing CryptoKey resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromCryptoKeyName(cryptoKeyName: string): string | number;
    /**
     * Parse the location from CryptoKey resource.
     *
     * @param {string} cryptoKeyName
     *   A fully-qualified path representing CryptoKey resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromCryptoKeyName(cryptoKeyName: string): string | number;
    /**
     * Parse the key_ring from CryptoKey resource.
     *
     * @param {string} cryptoKeyName
     *   A fully-qualified path representing CryptoKey resource.
     * @returns {string} A string representing the key_ring.
     */
    matchKeyRingFromCryptoKeyName(cryptoKeyName: string): string | number;
    /**
     * Parse the crypto_key from CryptoKey resource.
     *
     * @param {string} cryptoKeyName
     *   A fully-qualified path representing CryptoKey resource.
     * @returns {string} A string representing the crypto_key.
     */
    matchCryptoKeyFromCryptoKeyName(cryptoKeyName: string): string | number;
    /**
     * Return a fully-qualified cryptoKeyVersion resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_ring
     * @param {string} crypto_key
     * @param {string} crypto_key_version
     * @returns {string} Resource name string.
     */
    cryptoKeyVersionPath(project: string, location: string, keyRing: string, cryptoKey: string, cryptoKeyVersion: string): string;
    /**
     * Parse the project from CryptoKeyVersion resource.
     *
     * @param {string} cryptoKeyVersionName
     *   A fully-qualified path representing CryptoKeyVersion resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromCryptoKeyVersionName(cryptoKeyVersionName: string): string | number;
    /**
     * Parse the location from CryptoKeyVersion resource.
     *
     * @param {string} cryptoKeyVersionName
     *   A fully-qualified path representing CryptoKeyVersion resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromCryptoKeyVersionName(cryptoKeyVersionName: string): string | number;
    /**
     * Parse the key_ring from CryptoKeyVersion resource.
     *
     * @param {string} cryptoKeyVersionName
     *   A fully-qualified path representing CryptoKeyVersion resource.
     * @returns {string} A string representing the key_ring.
     */
    matchKeyRingFromCryptoKeyVersionName(cryptoKeyVersionName: string): string | number;
    /**
     * Parse the crypto_key from CryptoKeyVersion resource.
     *
     * @param {string} cryptoKeyVersionName
     *   A fully-qualified path representing CryptoKeyVersion resource.
     * @returns {string} A string representing the crypto_key.
     */
    matchCryptoKeyFromCryptoKeyVersionName(cryptoKeyVersionName: string): string | number;
    /**
     * Parse the crypto_key_version from CryptoKeyVersion resource.
     *
     * @param {string} cryptoKeyVersionName
     *   A fully-qualified path representing CryptoKeyVersion resource.
     * @returns {string} A string representing the crypto_key_version.
     */
    matchCryptoKeyVersionFromCryptoKeyVersionName(cryptoKeyVersionName: string): string | number;
    /**
     * Return a fully-qualified ekmConfig resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @returns {string} Resource name string.
     */
    ekmConfigPath(project: string, location: string): string;
    /**
     * Parse the project from EkmConfig resource.
     *
     * @param {string} ekmConfigName
     *   A fully-qualified path representing EkmConfig resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromEkmConfigName(ekmConfigName: string): string | number;
    /**
     * Parse the location from EkmConfig resource.
     *
     * @param {string} ekmConfigName
     *   A fully-qualified path representing EkmConfig resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromEkmConfigName(ekmConfigName: string): string | number;
    /**
     * Return a fully-qualified ekmConnection resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} ekm_connection
     * @returns {string} Resource name string.
     */
    ekmConnectionPath(project: string, location: string, ekmConnection: string): string;
    /**
     * Parse the project from EkmConnection resource.
     *
     * @param {string} ekmConnectionName
     *   A fully-qualified path representing EkmConnection resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromEkmConnectionName(ekmConnectionName: string): string | number;
    /**
     * Parse the location from EkmConnection resource.
     *
     * @param {string} ekmConnectionName
     *   A fully-qualified path representing EkmConnection resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromEkmConnectionName(ekmConnectionName: string): string | number;
    /**
     * Parse the ekm_connection from EkmConnection resource.
     *
     * @param {string} ekmConnectionName
     *   A fully-qualified path representing EkmConnection resource.
     * @returns {string} A string representing the ekm_connection.
     */
    matchEkmConnectionFromEkmConnectionName(ekmConnectionName: string): string | number;
    /**
     * Return a fully-qualified importJob resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_ring
     * @param {string} import_job
     * @returns {string} Resource name string.
     */
    importJobPath(project: string, location: string, keyRing: string, importJob: string): string;
    /**
     * Parse the project from ImportJob resource.
     *
     * @param {string} importJobName
     *   A fully-qualified path representing ImportJob resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromImportJobName(importJobName: string): string | number;
    /**
     * Parse the location from ImportJob resource.
     *
     * @param {string} importJobName
     *   A fully-qualified path representing ImportJob resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromImportJobName(importJobName: string): string | number;
    /**
     * Parse the key_ring from ImportJob resource.
     *
     * @param {string} importJobName
     *   A fully-qualified path representing ImportJob resource.
     * @returns {string} A string representing the key_ring.
     */
    matchKeyRingFromImportJobName(importJobName: string): string | number;
    /**
     * Parse the import_job from ImportJob resource.
     *
     * @param {string} importJobName
     *   A fully-qualified path representing ImportJob resource.
     * @returns {string} A string representing the import_job.
     */
    matchImportJobFromImportJobName(importJobName: string): string | number;
    /**
     * Return a fully-qualified keyHandle resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_handle
     * @returns {string} Resource name string.
     */
    keyHandlePath(project: string, location: string, keyHandle: string): string;
    /**
     * Parse the project from KeyHandle resource.
     *
     * @param {string} keyHandleName
     *   A fully-qualified path representing KeyHandle resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromKeyHandleName(keyHandleName: string): string | number;
    /**
     * Parse the location from KeyHandle resource.
     *
     * @param {string} keyHandleName
     *   A fully-qualified path representing KeyHandle resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromKeyHandleName(keyHandleName: string): string | number;
    /**
     * Parse the key_handle from KeyHandle resource.
     *
     * @param {string} keyHandleName
     *   A fully-qualified path representing KeyHandle resource.
     * @returns {string} A string representing the key_handle.
     */
    matchKeyHandleFromKeyHandleName(keyHandleName: string): string | number;
    /**
     * Return a fully-qualified keyRing resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_ring
     * @returns {string} Resource name string.
     */
    keyRingPath(project: string, location: string, keyRing: string): string;
    /**
     * Parse the project from KeyRing resource.
     *
     * @param {string} keyRingName
     *   A fully-qualified path representing KeyRing resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromKeyRingName(keyRingName: string): string | number;
    /**
     * Parse the location from KeyRing resource.
     *
     * @param {string} keyRingName
     *   A fully-qualified path representing KeyRing resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromKeyRingName(keyRingName: string): string | number;
    /**
     * Parse the key_ring from KeyRing resource.
     *
     * @param {string} keyRingName
     *   A fully-qualified path representing KeyRing resource.
     * @returns {string} A string representing the key_ring.
     */
    matchKeyRingFromKeyRingName(keyRingName: string): string | number;
    /**
     * Return a fully-qualified location resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @returns {string} Resource name string.
     */
    locationPath(project: string, location: string): string;
    /**
     * Parse the project from Location resource.
     *
     * @param {string} locationName
     *   A fully-qualified path representing Location resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromLocationName(locationName: string): string | number;
    /**
     * Parse the location from Location resource.
     *
     * @param {string} locationName
     *   A fully-qualified path representing Location resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromLocationName(locationName: string): string | number;
    /**
     * Return a fully-qualified publicKey resource name string.
     *
     * @param {string} project
     * @param {string} location
     * @param {string} key_ring
     * @param {string} crypto_key
     * @param {string} crypto_key_version
     * @returns {string} Resource name string.
     */
    publicKeyPath(project: string, location: string, keyRing: string, cryptoKey: string, cryptoKeyVersion: string): string;
    /**
     * Parse the project from PublicKey resource.
     *
     * @param {string} publicKeyName
     *   A fully-qualified path representing PublicKey resource.
     * @returns {string} A string representing the project.
     */
    matchProjectFromPublicKeyName(publicKeyName: string): string | number;
    /**
     * Parse the location from PublicKey resource.
     *
     * @param {string} publicKeyName
     *   A fully-qualified path representing PublicKey resource.
     * @returns {string} A string representing the location.
     */
    matchLocationFromPublicKeyName(publicKeyName: string): string | number;
    /**
     * Parse the key_ring from PublicKey resource.
     *
     * @param {string} publicKeyName
     *   A fully-qualified path representing PublicKey resource.
     * @returns {string} A string representing the key_ring.
     */
    matchKeyRingFromPublicKeyName(publicKeyName: string): string | number;
    /**
     * Parse the crypto_key from PublicKey resource.
     *
     * @param {string} publicKeyName
     *   A fully-qualified path representing PublicKey resource.
     * @returns {string} A string representing the crypto_key.
     */
    matchCryptoKeyFromPublicKeyName(publicKeyName: string): string | number;
    /**
     * Parse the crypto_key_version from PublicKey resource.
     *
     * @param {string} publicKeyName
     *   A fully-qualified path representing PublicKey resource.
     * @returns {string} A string representing the crypto_key_version.
     */
    matchCryptoKeyVersionFromPublicKeyName(publicKeyName: string): string | number;
    /**
     * Terminate the gRPC channel and close the client.
     *
     * The client will no longer be usable and all future behavior is undefined.
     * @returns {Promise} A promise that resolves when the client is closed.
     */
    close(): Promise<void>;
}
