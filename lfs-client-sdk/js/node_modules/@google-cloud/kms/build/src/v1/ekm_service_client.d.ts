import type * as gax from 'google-gax';
import type { Callback, CallOptions, Descriptors, ClientOptions, PaginationCallback, IamClient, IamProtos, LocationsClient, LocationProtos } from 'google-gax';
import { Transform } from 'stream';
import * as protos from '../../protos/protos';
/**
 *  Google Cloud Key Management EKM Service
 *
 *  Manages external cryptographic keys and operations using those keys.
 *  Implements a REST model with the following objects:
 *  * {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}
 * @class
 * @memberof v1
 */
export declare class EkmServiceClient {
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
    ekmServiceStub?: Promise<{
        [name: string]: Function;
    }>;
    /**
     * Construct an instance of EkmServiceClient.
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
     *     const client = new EkmServiceClient({fallback: true}, gax);
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
     * Returns metadata for a given
     * {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.EkmConnection.name|name} of the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.get_ekm_connection.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_GetEkmConnection_async
     */
    getEkmConnection(request?: protos.google.cloud.kms.v1.IGetEkmConnectionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConnection,
        protos.google.cloud.kms.v1.IGetEkmConnectionRequest | undefined,
        {} | undefined
    ]>;
    getEkmConnection(request: protos.google.cloud.kms.v1.IGetEkmConnectionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.IGetEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    getEkmConnection(request: protos.google.cloud.kms.v1.IGetEkmConnectionRequest, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.IGetEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Creates a new {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} in a given
     * Project and Location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}, in the format
     *   `projects/* /locations/*`.
     * @param {string} request.ekmConnectionId
     *   Required. It must be unique within a location and match the regular
     *   expression `[a-zA-Z0-9_-]{1,63}`.
     * @param {google.cloud.kms.v1.EkmConnection} request.ekmConnection
     *   Required. An {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} with
     *   initial field values.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.create_ekm_connection.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_CreateEkmConnection_async
     */
    createEkmConnection(request?: protos.google.cloud.kms.v1.ICreateEkmConnectionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConnection,
        protos.google.cloud.kms.v1.ICreateEkmConnectionRequest | undefined,
        {} | undefined
    ]>;
    createEkmConnection(request: protos.google.cloud.kms.v1.ICreateEkmConnectionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.ICreateEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    createEkmConnection(request: protos.google.cloud.kms.v1.ICreateEkmConnectionRequest, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.ICreateEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Updates an {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}'s metadata.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {google.cloud.kms.v1.EkmConnection} request.ekmConnection
     *   Required. {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} with updated
     *   values.
     * @param {google.protobuf.FieldMask} request.updateMask
     *   Required. List of fields to be updated in this request.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.update_ekm_connection.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_UpdateEkmConnection_async
     */
    updateEkmConnection(request?: protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConnection,
        protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest | undefined,
        {} | undefined
    ]>;
    updateEkmConnection(request: protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    updateEkmConnection(request: protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest, callback: Callback<protos.google.cloud.kms.v1.IEkmConnection, protos.google.cloud.kms.v1.IUpdateEkmConnectionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Returns the {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig} singleton resource
     * for a given project and location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.EkmConfig.name|name} of the
     *   {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.get_ekm_config.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_GetEkmConfig_async
     */
    getEkmConfig(request?: protos.google.cloud.kms.v1.IGetEkmConfigRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConfig,
        protos.google.cloud.kms.v1.IGetEkmConfigRequest | undefined,
        {} | undefined
    ]>;
    getEkmConfig(request: protos.google.cloud.kms.v1.IGetEkmConfigRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEkmConfig, protos.google.cloud.kms.v1.IGetEkmConfigRequest | null | undefined, {} | null | undefined>): void;
    getEkmConfig(request: protos.google.cloud.kms.v1.IGetEkmConfigRequest, callback: Callback<protos.google.cloud.kms.v1.IEkmConfig, protos.google.cloud.kms.v1.IGetEkmConfigRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Updates the {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig} singleton resource
     * for a given project and location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {google.cloud.kms.v1.EkmConfig} request.ekmConfig
     *   Required. {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig} with updated values.
     * @param {google.protobuf.FieldMask} request.updateMask
     *   Required. List of fields to be updated in this request.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EkmConfig|EkmConfig}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.update_ekm_config.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_UpdateEkmConfig_async
     */
    updateEkmConfig(request?: protos.google.cloud.kms.v1.IUpdateEkmConfigRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConfig,
        protos.google.cloud.kms.v1.IUpdateEkmConfigRequest | undefined,
        {} | undefined
    ]>;
    updateEkmConfig(request: protos.google.cloud.kms.v1.IUpdateEkmConfigRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEkmConfig, protos.google.cloud.kms.v1.IUpdateEkmConfigRequest | null | undefined, {} | null | undefined>): void;
    updateEkmConfig(request: protos.google.cloud.kms.v1.IUpdateEkmConfigRequest, callback: Callback<protos.google.cloud.kms.v1.IEkmConfig, protos.google.cloud.kms.v1.IUpdateEkmConfigRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Verifies that Cloud KMS can successfully connect to the external key
     * manager specified by an {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     * If there is an error connecting to the EKM, this method returns a
     * FAILED_PRECONDITION status containing structured information as described
     * at https://cloud.google.com/kms/docs/reference/ekm_errors.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.EkmConnection.name|name} of the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} to verify.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.VerifyConnectivityResponse|VerifyConnectivityResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.verify_connectivity.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_VerifyConnectivity_async
     */
    verifyConnectivity(request?: protos.google.cloud.kms.v1.IVerifyConnectivityRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IVerifyConnectivityResponse,
        protos.google.cloud.kms.v1.IVerifyConnectivityRequest | undefined,
        {} | undefined
    ]>;
    verifyConnectivity(request: protos.google.cloud.kms.v1.IVerifyConnectivityRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IVerifyConnectivityResponse, protos.google.cloud.kms.v1.IVerifyConnectivityRequest | null | undefined, {} | null | undefined>): void;
    verifyConnectivity(request: protos.google.cloud.kms.v1.IVerifyConnectivityRequest, callback: Callback<protos.google.cloud.kms.v1.IVerifyConnectivityResponse, protos.google.cloud.kms.v1.IVerifyConnectivityRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Lists {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to list, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} can
     *   subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order.  For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is Array of {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed and will merge results from all the pages into this array.
     *   Note that it can affect your quota.
     *   We recommend using `listEkmConnectionsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listEkmConnections(request?: protos.google.cloud.kms.v1.IListEkmConnectionsRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEkmConnection[],
        protos.google.cloud.kms.v1.IListEkmConnectionsRequest | null,
        protos.google.cloud.kms.v1.IListEkmConnectionsResponse
    ]>;
    listEkmConnections(request: protos.google.cloud.kms.v1.IListEkmConnectionsRequest, options: CallOptions, callback: PaginationCallback<protos.google.cloud.kms.v1.IListEkmConnectionsRequest, protos.google.cloud.kms.v1.IListEkmConnectionsResponse | null | undefined, protos.google.cloud.kms.v1.IEkmConnection>): void;
    listEkmConnections(request: protos.google.cloud.kms.v1.IListEkmConnectionsRequest, callback: PaginationCallback<protos.google.cloud.kms.v1.IListEkmConnectionsRequest, protos.google.cloud.kms.v1.IListEkmConnectionsResponse | null | undefined, protos.google.cloud.kms.v1.IEkmConnection>): void;
    /**
     * Equivalent to `method.name.toCamelCase()`, but returns a NodeJS Stream object.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to list, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} can
     *   subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order.  For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Stream}
     *   An object stream which emits an object representing {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection} on 'data' event.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed. Note that it can affect your quota.
     *   We recommend using `listEkmConnectionsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listEkmConnectionsStream(request?: protos.google.cloud.kms.v1.IListEkmConnectionsRequest, options?: CallOptions): Transform;
    /**
     * Equivalent to `listEkmConnections`, but returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to list, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnections} can
     *   subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListEkmConnectionsResponse.next_page_token|ListEkmConnectionsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order.  For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Object}
     *   An iterable Object that allows {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | async iteration }.
     *   When you iterate the returned iterable, each element will be an object representing
     *   {@link protos.google.cloud.kms.v1.EkmConnection|EkmConnection}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/ekm_service.list_ekm_connections.js</caption>
     * region_tag:cloudkms_v1_generated_EkmService_ListEkmConnections_async
     */
    listEkmConnectionsAsync(request?: protos.google.cloud.kms.v1.IListEkmConnectionsRequest, options?: CallOptions): AsyncIterable<protos.google.cloud.kms.v1.IEkmConnection>;
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
