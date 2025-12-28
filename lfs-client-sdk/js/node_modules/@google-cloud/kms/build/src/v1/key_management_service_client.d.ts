import type * as gax from 'google-gax';
import type { Callback, CallOptions, Descriptors, ClientOptions, PaginationCallback, IamClient, IamProtos, LocationsClient, LocationProtos } from 'google-gax';
import { Transform } from 'stream';
import * as protos from '../../protos/protos';
/**
 *  Google Cloud Key Management Service
 *
 *  Manages cryptographic keys and operations using those keys. Implements a REST
 *  model with the following objects:
 *
 *  * {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
 *  * {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}
 *  * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}
 *  * {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}
 *
 *  If you are using manual gRPC libraries, see
 *  [Using gRPC with Cloud KMS](https://cloud.google.com/kms/docs/grpc).
 * @class
 * @memberof v1
 */
export declare class KeyManagementServiceClient {
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
    keyManagementServiceStub?: Promise<{
        [name: string]: Function;
    }>;
    /**
     * Construct an instance of KeyManagementServiceClient.
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
     *     const client = new KeyManagementServiceClient({fallback: true}, gax);
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
     * Returns metadata for a given {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.KeyRing.name|name} of the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRing} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.get_key_ring.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GetKeyRing_async
     */
    getKeyRing(request?: protos.google.cloud.kms.v1.IGetKeyRingRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IKeyRing,
        protos.google.cloud.kms.v1.IGetKeyRingRequest | undefined,
        {} | undefined
    ]>;
    getKeyRing(request: protos.google.cloud.kms.v1.IGetKeyRingRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IKeyRing, protos.google.cloud.kms.v1.IGetKeyRingRequest | null | undefined, {} | null | undefined>): void;
    getKeyRing(request: protos.google.cloud.kms.v1.IGetKeyRingRequest, callback: Callback<protos.google.cloud.kms.v1.IKeyRing, protos.google.cloud.kms.v1.IGetKeyRingRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Returns metadata for a given {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}, as
     * well as its {@link protos.google.cloud.kms.v1.CryptoKey.primary|primary}
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.CryptoKey.name|name} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.get_crypto_key.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GetCryptoKey_async
     */
    getCryptoKey(request?: protos.google.cloud.kms.v1.IGetCryptoKeyRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKey,
        protos.google.cloud.kms.v1.IGetCryptoKeyRequest | undefined,
        {} | undefined
    ]>;
    getCryptoKey(request: protos.google.cloud.kms.v1.IGetCryptoKeyRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IGetCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    getCryptoKey(request: protos.google.cloud.kms.v1.IGetCryptoKeyRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IGetCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Returns metadata for a given
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.CryptoKeyVersion.name|name} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.get_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GetCryptoKeyVersion_async
     */
    getCryptoKeyVersion(request?: protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    getCryptoKeyVersion(request: protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    getCryptoKeyVersion(request: protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IGetCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Returns the public key for the given
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}. The
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} must be
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.ASYMMETRIC_SIGN|ASYMMETRIC_SIGN}
     * or
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.ASYMMETRIC_DECRYPT|ASYMMETRIC_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.CryptoKeyVersion.name|name} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} public key to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.PublicKey|PublicKey}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.get_public_key.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GetPublicKey_async
     */
    getPublicKey(request?: protos.google.cloud.kms.v1.IGetPublicKeyRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IPublicKey,
        protos.google.cloud.kms.v1.IGetPublicKeyRequest | undefined,
        {} | undefined
    ]>;
    getPublicKey(request: protos.google.cloud.kms.v1.IGetPublicKeyRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IPublicKey, protos.google.cloud.kms.v1.IGetPublicKeyRequest | null | undefined, {} | null | undefined>): void;
    getPublicKey(request: protos.google.cloud.kms.v1.IGetPublicKeyRequest, callback: Callback<protos.google.cloud.kms.v1.IPublicKey, protos.google.cloud.kms.v1.IGetPublicKeyRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Returns metadata for a given {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The {@link protos.google.cloud.kms.v1.ImportJob.name|name} of the
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJob} to get.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.get_import_job.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GetImportJob_async
     */
    getImportJob(request?: protos.google.cloud.kms.v1.IGetImportJobRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IImportJob,
        protos.google.cloud.kms.v1.IGetImportJobRequest | undefined,
        {} | undefined
    ]>;
    getImportJob(request: protos.google.cloud.kms.v1.IGetImportJobRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IImportJob, protos.google.cloud.kms.v1.IGetImportJobRequest | null | undefined, {} | null | undefined>): void;
    getImportJob(request: protos.google.cloud.kms.v1.IGetImportJobRequest, callback: Callback<protos.google.cloud.kms.v1.IImportJob, protos.google.cloud.kms.v1.IGetImportJobRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Create a new {@link protos.google.cloud.kms.v1.KeyRing|KeyRing} in a given Project and
     * Location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings}, in the format
     *   `projects/* /locations/*`.
     * @param {string} request.keyRingId
     *   Required. It must be unique within a location and match the regular
     *   expression `[a-zA-Z0-9_-]{1,63}`
     * @param {google.cloud.kms.v1.KeyRing} request.keyRing
     *   Required. A {@link protos.google.cloud.kms.v1.KeyRing|KeyRing} with initial field
     *   values.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.create_key_ring.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_CreateKeyRing_async
     */
    createKeyRing(request?: protos.google.cloud.kms.v1.ICreateKeyRingRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IKeyRing,
        protos.google.cloud.kms.v1.ICreateKeyRingRequest | undefined,
        {} | undefined
    ]>;
    createKeyRing(request: protos.google.cloud.kms.v1.ICreateKeyRingRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IKeyRing, protos.google.cloud.kms.v1.ICreateKeyRingRequest | null | undefined, {} | null | undefined>): void;
    createKeyRing(request: protos.google.cloud.kms.v1.ICreateKeyRingRequest, callback: Callback<protos.google.cloud.kms.v1.IKeyRing, protos.google.cloud.kms.v1.ICreateKeyRingRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Create a new {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} within a
     * {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} and
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersionTemplate.algorithm|CryptoKey.version_template.algorithm}
     * are required.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The {@link protos.google.cloud.kms.v1.KeyRing.name|name} of the KeyRing
     *   associated with the {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys}.
     * @param {string} request.cryptoKeyId
     *   Required. It must be unique within a KeyRing and match the regular
     *   expression `[a-zA-Z0-9_-]{1,63}`
     * @param {google.cloud.kms.v1.CryptoKey} request.cryptoKey
     *   Required. A {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} with initial field
     *   values.
     * @param {boolean} request.skipInitialVersionCreation
     *   If set to true, the request will create a
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} without any
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}. You must
     *   manually call
     *   {@link protos.google.cloud.kms.v1.KeyManagementService.CreateCryptoKeyVersion|CreateCryptoKeyVersion}
     *   or
     *   {@link protos.google.cloud.kms.v1.KeyManagementService.ImportCryptoKeyVersion|ImportCryptoKeyVersion}
     *   before you can use this {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.create_crypto_key.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_CreateCryptoKey_async
     */
    createCryptoKey(request?: protos.google.cloud.kms.v1.ICreateCryptoKeyRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKey,
        protos.google.cloud.kms.v1.ICreateCryptoKeyRequest | undefined,
        {} | undefined
    ]>;
    createCryptoKey(request: protos.google.cloud.kms.v1.ICreateCryptoKeyRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.ICreateCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    createCryptoKey(request: protos.google.cloud.kms.v1.ICreateCryptoKeyRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.ICreateCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Create a new {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} in a
     * {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *
     * The server will assign the next sequential id. If unset,
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.state|state} will be set to
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.ENABLED|ENABLED}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The {@link protos.google.cloud.kms.v1.CryptoKey.name|name} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} associated with the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion} request.cryptoKeyVersion
     *   Required. A {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} with
     *   initial field values.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.create_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_CreateCryptoKeyVersion_async
     */
    createCryptoKeyVersion(request?: protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    createCryptoKeyVersion(request: protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    createCryptoKeyVersion(request: protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.ICreateCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Import wrapped key material into a
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *
     * All requests must specify a {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}. If
     * a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} is additionally
     * specified in the request, key material will be reimported into that
     * version. Otherwise, a new version will be created, and will be assigned the
     * next sequential id within the {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The {@link protos.google.cloud.kms.v1.CryptoKey.name|name} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to be imported into.
     *
     *   The create permission is only required on this key when creating a new
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     * @param {string} [request.cryptoKeyVersion]
     *   Optional. The optional {@link protos.google.cloud.kms.v1.CryptoKeyVersion.name|name} of
     *   an existing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to
     *   target for an import operation. If this field is not present, a new
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} containing the
     *   supplied key material is created.
     *
     *   If this field is present, the supplied key material is imported into
     *   the existing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}. To
     *   import into an existing
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}, the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} must be a child of
     *   {@link protos.google.cloud.kms.v1.ImportCryptoKeyVersionRequest.parent|ImportCryptoKeyVersionRequest.parent},
     *   have been previously created via {@link protos.|ImportCryptoKeyVersion}, and be in
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROYED|DESTROYED}
     *   or
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.IMPORT_FAILED|IMPORT_FAILED}
     *   state. The key material and algorithm must match the previous
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} exactly if the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} has ever contained
     *   key material.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm} request.algorithm
     *   Required. The
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm|algorithm}
     *   of the key being imported. This does not need to match the
     *   {@link protos.google.cloud.kms.v1.CryptoKey.version_template|version_template} of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} this version imports into.
     * @param {string} request.importJob
     *   Required. The {@link protos.google.cloud.kms.v1.ImportJob.name|name} of the
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJob} that was used to wrap this key
     *   material.
     * @param {Buffer} [request.wrappedKey]
     *   Optional. The wrapped key material to import.
     *
     *   Before wrapping, key material must be formatted. If importing symmetric key
     *   material, the expected key material format is plain bytes. If importing
     *   asymmetric key material, the expected key material format is PKCS#8-encoded
     *   DER (the PrivateKeyInfo structure from RFC 5208).
     *
     *   When wrapping with import methods
     *   ({@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_3072_SHA1_AES_256|RSA_OAEP_3072_SHA1_AES_256}
     *   or
     *   {@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_4096_SHA1_AES_256|RSA_OAEP_4096_SHA1_AES_256}
     *   or
     *   {@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_3072_SHA256_AES_256|RSA_OAEP_3072_SHA256_AES_256}
     *   or
     *   {@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_4096_SHA256_AES_256|RSA_OAEP_4096_SHA256_AES_256}),
     *
     *   this field must contain the concatenation of:
     *   <ol>
     *     <li>An ephemeral AES-256 wrapping key wrapped with the
     *         {@link protos.google.cloud.kms.v1.ImportJob.public_key|public_key} using
     *         RSAES-OAEP with SHA-1/SHA-256, MGF1 with SHA-1/SHA-256, and an empty
     *         label.
     *     </li>
     *     <li>The formatted key to be imported, wrapped with the ephemeral AES-256
     *         key using AES-KWP (RFC 5649).
     *     </li>
     *   </ol>
     *
     *   This format is the same as the format produced by PKCS#11 mechanism
     *   CKM_RSA_AES_KEY_WRAP.
     *
     *   When wrapping with import methods
     *   ({@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_3072_SHA256|RSA_OAEP_3072_SHA256}
     *   or
     *   {@link protos.google.cloud.kms.v1.ImportJob.ImportMethod.RSA_OAEP_4096_SHA256|RSA_OAEP_4096_SHA256}),
     *
     *   this field must contain the formatted key to be imported, wrapped with the
     *   {@link protos.google.cloud.kms.v1.ImportJob.public_key|public_key} using RSAES-OAEP
     *   with SHA-256, MGF1 with SHA-256, and an empty label.
     * @param {Buffer} [request.rsaAesWrappedKey]
     *   Optional. This field has the same meaning as
     *   {@link protos.google.cloud.kms.v1.ImportCryptoKeyVersionRequest.wrapped_key|wrapped_key}.
     *   Prefer to use that field in new work. Either that field or this field
     *   (but not both) must be specified.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.import_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_ImportCryptoKeyVersion_async
     */
    importCryptoKeyVersion(request?: protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    importCryptoKeyVersion(request: protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    importCryptoKeyVersion(request: protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IImportCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Create a new {@link protos.google.cloud.kms.v1.ImportJob|ImportJob} within a
     * {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *
     * {@link protos.google.cloud.kms.v1.ImportJob.import_method|ImportJob.import_method} is
     * required.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The {@link protos.google.cloud.kms.v1.KeyRing.name|name} of the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRing} associated with the
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs}.
     * @param {string} request.importJobId
     *   Required. It must be unique within a KeyRing and match the regular
     *   expression `[a-zA-Z0-9_-]{1,63}`
     * @param {google.cloud.kms.v1.ImportJob} request.importJob
     *   Required. An {@link protos.google.cloud.kms.v1.ImportJob|ImportJob} with initial field
     *   values.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.create_import_job.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_CreateImportJob_async
     */
    createImportJob(request?: protos.google.cloud.kms.v1.ICreateImportJobRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IImportJob,
        protos.google.cloud.kms.v1.ICreateImportJobRequest | undefined,
        {} | undefined
    ]>;
    createImportJob(request: protos.google.cloud.kms.v1.ICreateImportJobRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IImportJob, protos.google.cloud.kms.v1.ICreateImportJobRequest | null | undefined, {} | null | undefined>): void;
    createImportJob(request: protos.google.cloud.kms.v1.ICreateImportJobRequest, callback: Callback<protos.google.cloud.kms.v1.IImportJob, protos.google.cloud.kms.v1.ICreateImportJobRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Update a {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {google.cloud.kms.v1.CryptoKey} request.cryptoKey
     *   Required. {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} with updated values.
     * @param {google.protobuf.FieldMask} request.updateMask
     *   Required. List of fields to be updated in this request.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.update_crypto_key.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_UpdateCryptoKey_async
     */
    updateCryptoKey(request?: protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKey,
        protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest | undefined,
        {} | undefined
    ]>;
    updateCryptoKey(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    updateCryptoKey(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IUpdateCryptoKeyRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Update a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}'s
     * metadata.
     *
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.state|state} may be changed between
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.ENABLED|ENABLED}
     * and
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DISABLED|DISABLED}
     * using this method. See
     * {@link protos.google.cloud.kms.v1.KeyManagementService.DestroyCryptoKeyVersion|DestroyCryptoKeyVersion}
     * and
     * {@link protos.google.cloud.kms.v1.KeyManagementService.RestoreCryptoKeyVersion|RestoreCryptoKeyVersion}
     * to move between other states.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {google.cloud.kms.v1.CryptoKeyVersion} request.cryptoKeyVersion
     *   Required. {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} with
     *   updated values.
     * @param {google.protobuf.FieldMask} request.updateMask
     *   Required. List of fields to be updated in this request.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.update_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_UpdateCryptoKeyVersion_async
     */
    updateCryptoKeyVersion(request?: protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    updateCryptoKeyVersion(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    updateCryptoKeyVersion(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IUpdateCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Update the version of a {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} that
     * will be used in
     * {@link protos.google.cloud.kms.v1.KeyManagementService.Encrypt|Encrypt}.
     *
     * Returns an error if called on a key whose purpose is not
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT|ENCRYPT_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to update.
     * @param {string} request.cryptoKeyVersionId
     *   Required. The id of the child
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use as primary.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.update_crypto_key_primary_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_UpdateCryptoKeyPrimaryVersion_async
     */
    updateCryptoKeyPrimaryVersion(request?: protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKey,
        (protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest | undefined),
        {} | undefined
    ]>;
    updateCryptoKeyPrimaryVersion(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest | null | undefined, {} | null | undefined>): void;
    updateCryptoKeyPrimaryVersion(request: protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKey, protos.google.cloud.kms.v1.IUpdateCryptoKeyPrimaryVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Schedule a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} for
     * destruction.
     *
     * Upon calling this method,
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.state|CryptoKeyVersion.state} will
     * be set to
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED|DESTROY_SCHEDULED},
     * and {@link protos.google.cloud.kms.v1.CryptoKeyVersion.destroy_time|destroy_time} will
     * be set to the time
     * {@link protos.google.cloud.kms.v1.CryptoKey.destroy_scheduled_duration|destroy_scheduled_duration}
     * in the future. At that time, the
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.state|state} will automatically
     * change to
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROYED|DESTROYED},
     * and the key material will be irrevocably destroyed.
     *
     * Before the
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.destroy_time|destroy_time} is
     * reached,
     * {@link protos.google.cloud.kms.v1.KeyManagementService.RestoreCryptoKeyVersion|RestoreCryptoKeyVersion}
     * may be called to reverse the process.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to destroy.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.destroy_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_DestroyCryptoKeyVersion_async
     */
    destroyCryptoKeyVersion(request?: protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    destroyCryptoKeyVersion(request: protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    destroyCryptoKeyVersion(request: protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IDestroyCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Restore a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} in the
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED|DESTROY_SCHEDULED}
     * state.
     *
     * Upon restoration of the CryptoKeyVersion,
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.state|state} will be set to
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionState.DISABLED|DISABLED},
     * and {@link protos.google.cloud.kms.v1.CryptoKeyVersion.destroy_time|destroy_time} will
     * be cleared.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to restore.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.restore_crypto_key_version.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_RestoreCryptoKeyVersion_async
     */
    restoreCryptoKeyVersion(request?: protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion,
        protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest | undefined,
        {} | undefined
    ]>;
    restoreCryptoKeyVersion(request: protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    restoreCryptoKeyVersion(request: protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest, callback: Callback<protos.google.cloud.kms.v1.ICryptoKeyVersion, protos.google.cloud.kms.v1.IRestoreCryptoKeyVersionRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Encrypts data, so that it can only be recovered by a call to
     * {@link protos.google.cloud.kms.v1.KeyManagementService.Decrypt|Decrypt}. The
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} must be
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT|ENCRYPT_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} or
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   encryption.
     *
     *   If a {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} is specified, the server
     *   will use its {@link protos.google.cloud.kms.v1.CryptoKey.primary|primary version}.
     * @param {Buffer} request.plaintext
     *   Required. The data to encrypt. Must be no larger than 64KiB.
     *
     *   The maximum size depends on the key version's
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersionTemplate.protection_level|protection_level}.
     *   For {@link protos.google.cloud.kms.v1.ProtectionLevel.SOFTWARE|SOFTWARE},
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.EXTERNAL|EXTERNAL}, and
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.EXTERNAL_VPC|EXTERNAL_VPC} keys, the
     *   plaintext must be no larger than 64KiB. For
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.HSM|HSM} keys, the combined length of
     *   the plaintext and additional_authenticated_data fields must be no larger
     *   than 8KiB.
     * @param {Buffer} [request.additionalAuthenticatedData]
     *   Optional. Optional data that, if specified, must also be provided during
     *   decryption through
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.additional_authenticated_data|DecryptRequest.additional_authenticated_data}.
     *
     *   The maximum size depends on the key version's
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersionTemplate.protection_level|protection_level}.
     *   For {@link protos.google.cloud.kms.v1.ProtectionLevel.SOFTWARE|SOFTWARE},
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.EXTERNAL|EXTERNAL}, and
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.EXTERNAL_VPC|EXTERNAL_VPC} keys the
     *   AAD must be no larger than 64KiB. For
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.HSM|HSM} keys, the combined length of
     *   the plaintext and additional_authenticated_data fields must be no larger
     *   than 8KiB.
     * @param {google.protobuf.Int64Value} [request.plaintextCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.plaintext|EncryptRequest.plaintext}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.plaintext|EncryptRequest.plaintext}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.EncryptRequest.plaintext|EncryptRequest.plaintext})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.plaintext_crc32c|EncryptRequest.plaintext_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {google.protobuf.Int64Value} [request.additionalAuthenticatedDataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.additional_authenticated_data|EncryptRequest.additional_authenticated_data}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.additional_authenticated_data|EncryptRequest.additional_authenticated_data}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.EncryptRequest.additional_authenticated_data|EncryptRequest.additional_authenticated_data})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.additional_authenticated_data_crc32c|EncryptRequest.additional_authenticated_data_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.EncryptResponse|EncryptResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.encrypt.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_Encrypt_async
     */
    encrypt(request?: protos.google.cloud.kms.v1.IEncryptRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IEncryptResponse,
        protos.google.cloud.kms.v1.IEncryptRequest | undefined,
        {} | undefined
    ]>;
    encrypt(request: protos.google.cloud.kms.v1.IEncryptRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IEncryptResponse, protos.google.cloud.kms.v1.IEncryptRequest | null | undefined, {} | null | undefined>): void;
    encrypt(request: protos.google.cloud.kms.v1.IEncryptRequest, callback: Callback<protos.google.cloud.kms.v1.IEncryptResponse, protos.google.cloud.kms.v1.IEncryptRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Decrypts data that was protected by
     * {@link protos.google.cloud.kms.v1.KeyManagementService.Encrypt|Encrypt}. The
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} must be
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT|ENCRYPT_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to use for decryption. The
     *   server will choose the appropriate version.
     * @param {Buffer} request.ciphertext
     *   Required. The encrypted data originally returned in
     *   {@link protos.google.cloud.kms.v1.EncryptResponse.ciphertext|EncryptResponse.ciphertext}.
     * @param {Buffer} [request.additionalAuthenticatedData]
     *   Optional. Optional data that must match the data originally supplied in
     *   {@link protos.google.cloud.kms.v1.EncryptRequest.additional_authenticated_data|EncryptRequest.additional_authenticated_data}.
     * @param {google.protobuf.Int64Value} [request.ciphertextCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.ciphertext|DecryptRequest.ciphertext}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.ciphertext|DecryptRequest.ciphertext}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.DecryptRequest.ciphertext|DecryptRequest.ciphertext})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.ciphertext_crc32c|DecryptRequest.ciphertext_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {google.protobuf.Int64Value} [request.additionalAuthenticatedDataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.additional_authenticated_data|DecryptRequest.additional_authenticated_data}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.additional_authenticated_data|DecryptRequest.additional_authenticated_data}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.DecryptRequest.additional_authenticated_data|DecryptRequest.additional_authenticated_data})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.DecryptRequest.additional_authenticated_data_crc32c|DecryptRequest.additional_authenticated_data_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.DecryptResponse|DecryptResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.decrypt.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_Decrypt_async
     */
    decrypt(request?: protos.google.cloud.kms.v1.IDecryptRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IDecryptResponse,
        protos.google.cloud.kms.v1.IDecryptRequest | undefined,
        {} | undefined
    ]>;
    decrypt(request: protos.google.cloud.kms.v1.IDecryptRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IDecryptResponse, protos.google.cloud.kms.v1.IDecryptRequest | null | undefined, {} | null | undefined>): void;
    decrypt(request: protos.google.cloud.kms.v1.IDecryptRequest, callback: Callback<protos.google.cloud.kms.v1.IDecryptResponse, protos.google.cloud.kms.v1.IDecryptRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Encrypts data using portable cryptographic primitives. Most users should
     * choose {@link protos.google.cloud.kms.v1.KeyManagementService.Encrypt|Encrypt} and
     * {@link protos.google.cloud.kms.v1.KeyManagementService.Decrypt|Decrypt} rather than
     * their raw counterparts. The
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} must be
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.RAW_ENCRYPT_DECRYPT|RAW_ENCRYPT_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   encryption.
     * @param {Buffer} request.plaintext
     *   Required. The data to encrypt. Must be no larger than 64KiB.
     *
     *   The maximum size depends on the key version's
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersionTemplate.protection_level|protection_level}.
     *   For {@link protos.google.cloud.kms.v1.ProtectionLevel.SOFTWARE|SOFTWARE} keys, the
     *   plaintext must be no larger than 64KiB. For
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.HSM|HSM} keys, the combined length of
     *   the plaintext and additional_authenticated_data fields must be no larger
     *   than 8KiB.
     * @param {Buffer} [request.additionalAuthenticatedData]
     *   Optional. Optional data that, if specified, must also be provided during
     *   decryption through
     *   {@link protos.google.cloud.kms.v1.RawDecryptRequest.additional_authenticated_data|RawDecryptRequest.additional_authenticated_data}.
     *
     *   This field may only be used in conjunction with an
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion.algorithm|algorithm} that accepts
     *   additional authenticated data (for example, AES-GCM).
     *
     *   The maximum size depends on the key version's
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersionTemplate.protection_level|protection_level}.
     *   For {@link protos.google.cloud.kms.v1.ProtectionLevel.SOFTWARE|SOFTWARE} keys, the
     *   plaintext must be no larger than 64KiB. For
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.HSM|HSM} keys, the combined length of
     *   the plaintext and additional_authenticated_data fields must be no larger
     *   than 8KiB.
     * @param {google.protobuf.Int64Value} [request.plaintextCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawEncryptRequest.plaintext|RawEncryptRequest.plaintext}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received plaintext using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that CRC32C(plaintext) is equal
     *   to plaintext_crc32c, and if so, perform a limited number of retries. A
     *   persistent mismatch may indicate an issue in your computation of the CRC32C
     *   checksum. Note: This field is defined as int64 for reasons of compatibility
     *   across different languages. However, it is a non-negative integer, which
     *   will never exceed 2^32-1, and can be safely downconverted to uint32 in
     *   languages that support this type.
     * @param {google.protobuf.Int64Value} [request.additionalAuthenticatedDataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawEncryptRequest.additional_authenticated_data|RawEncryptRequest.additional_authenticated_data}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received additional_authenticated_data using
     *   this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C(additional_authenticated_data) is equal to
     *   additional_authenticated_data_crc32c, and if so, perform
     *   a limited number of retries. A persistent mismatch may indicate an issue in
     *   your computation of the CRC32C checksum.
     *   Note: This field is defined as int64 for reasons of compatibility across
     *   different languages. However, it is a non-negative integer, which will
     *   never exceed 2^32-1, and can be safely downconverted to uint32 in languages
     *   that support this type.
     * @param {Buffer} [request.initializationVector]
     *   Optional. A customer-supplied initialization vector that will be used for
     *   encryption. If it is not provided for AES-CBC and AES-CTR, one will be
     *   generated. It will be returned in
     *   {@link protos.google.cloud.kms.v1.RawEncryptResponse.initialization_vector|RawEncryptResponse.initialization_vector}.
     * @param {google.protobuf.Int64Value} [request.initializationVectorCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawEncryptRequest.initialization_vector|RawEncryptRequest.initialization_vector}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received initialization_vector using this
     *   checksum. {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C(initialization_vector) is equal to
     *   initialization_vector_crc32c, and if so, perform
     *   a limited number of retries. A persistent mismatch may indicate an issue in
     *   your computation of the CRC32C checksum.
     *   Note: This field is defined as int64 for reasons of compatibility across
     *   different languages. However, it is a non-negative integer, which will
     *   never exceed 2^32-1, and can be safely downconverted to uint32 in languages
     *   that support this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.RawEncryptResponse|RawEncryptResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.raw_encrypt.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_RawEncrypt_async
     */
    rawEncrypt(request?: protos.google.cloud.kms.v1.IRawEncryptRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IRawEncryptResponse,
        protos.google.cloud.kms.v1.IRawEncryptRequest | undefined,
        {} | undefined
    ]>;
    rawEncrypt(request: protos.google.cloud.kms.v1.IRawEncryptRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IRawEncryptResponse, protos.google.cloud.kms.v1.IRawEncryptRequest | null | undefined, {} | null | undefined>): void;
    rawEncrypt(request: protos.google.cloud.kms.v1.IRawEncryptRequest, callback: Callback<protos.google.cloud.kms.v1.IRawEncryptResponse, protos.google.cloud.kms.v1.IRawEncryptRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Decrypts data that was originally encrypted using a raw cryptographic
     * mechanism. The {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose}
     * must be
     * {@link protos.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose.RAW_ENCRYPT_DECRYPT|RAW_ENCRYPT_DECRYPT}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   decryption.
     * @param {Buffer} request.ciphertext
     *   Required. The encrypted data originally returned in
     *   {@link protos.google.cloud.kms.v1.RawEncryptResponse.ciphertext|RawEncryptResponse.ciphertext}.
     * @param {Buffer} [request.additionalAuthenticatedData]
     *   Optional. Optional data that must match the data originally supplied in
     *   {@link protos.google.cloud.kms.v1.RawEncryptRequest.additional_authenticated_data|RawEncryptRequest.additional_authenticated_data}.
     * @param {Buffer} request.initializationVector
     *   Required. The initialization vector (IV) used during encryption, which must
     *   match the data originally provided in
     *   {@link protos.google.cloud.kms.v1.RawEncryptResponse.initialization_vector|RawEncryptResponse.initialization_vector}.
     * @param {number} request.tagLength
     *   The length of the authentication tag that is appended to the end of
     *   the ciphertext. If unspecified (0), the default value for the key's
     *   algorithm will be used (for AES-GCM, the default value is 16).
     * @param {google.protobuf.Int64Value} [request.ciphertextCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawDecryptRequest.ciphertext|RawDecryptRequest.ciphertext}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received ciphertext using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that CRC32C(ciphertext) is equal
     *   to ciphertext_crc32c, and if so, perform a limited number of retries. A
     *   persistent mismatch may indicate an issue in your computation of the CRC32C
     *   checksum. Note: This field is defined as int64 for reasons of compatibility
     *   across different languages. However, it is a non-negative integer, which
     *   will never exceed 2^32-1, and can be safely downconverted to uint32 in
     *   languages that support this type.
     * @param {google.protobuf.Int64Value} [request.additionalAuthenticatedDataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawDecryptRequest.additional_authenticated_data|RawDecryptRequest.additional_authenticated_data}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received additional_authenticated_data using
     *   this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C(additional_authenticated_data) is equal to
     *   additional_authenticated_data_crc32c, and if so, perform
     *   a limited number of retries. A persistent mismatch may indicate an issue in
     *   your computation of the CRC32C checksum.
     *   Note: This field is defined as int64 for reasons of compatibility across
     *   different languages. However, it is a non-negative integer, which will
     *   never exceed 2^32-1, and can be safely downconverted to uint32 in languages
     *   that support this type.
     * @param {google.protobuf.Int64Value} [request.initializationVectorCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.RawDecryptRequest.initialization_vector|RawDecryptRequest.initialization_vector}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received initialization_vector using this
     *   checksum. {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C(initialization_vector) is equal to initialization_vector_crc32c, and
     *   if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum.
     *   Note: This field is defined as int64 for reasons of compatibility across
     *   different languages. However, it is a non-negative integer, which will
     *   never exceed 2^32-1, and can be safely downconverted to uint32 in languages
     *   that support this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.RawDecryptResponse|RawDecryptResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.raw_decrypt.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_RawDecrypt_async
     */
    rawDecrypt(request?: protos.google.cloud.kms.v1.IRawDecryptRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IRawDecryptResponse,
        protos.google.cloud.kms.v1.IRawDecryptRequest | undefined,
        {} | undefined
    ]>;
    rawDecrypt(request: protos.google.cloud.kms.v1.IRawDecryptRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IRawDecryptResponse, protos.google.cloud.kms.v1.IRawDecryptRequest | null | undefined, {} | null | undefined>): void;
    rawDecrypt(request: protos.google.cloud.kms.v1.IRawDecryptRequest, callback: Callback<protos.google.cloud.kms.v1.IRawDecryptResponse, protos.google.cloud.kms.v1.IRawDecryptRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Signs data using a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}
     * with {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose}
     * ASYMMETRIC_SIGN, producing a signature that can be verified with the public
     * key retrieved from
     * {@link protos.google.cloud.kms.v1.KeyManagementService.GetPublicKey|GetPublicKey}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   signing.
     * @param {google.cloud.kms.v1.Digest} [request.digest]
     *   Optional. The digest of the data to sign. The digest must be produced with
     *   the same digest algorithm as specified by the key version's
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion.algorithm|algorithm}.
     *
     *   This field may not be supplied if
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.data|AsymmetricSignRequest.data}
     *   is supplied.
     * @param {google.protobuf.Int64Value} [request.digestCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.digest|AsymmetricSignRequest.digest}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.digest|AsymmetricSignRequest.digest}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.AsymmetricSignRequest.digest|AsymmetricSignRequest.digest})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.digest_crc32c|AsymmetricSignRequest.digest_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {Buffer} [request.data]
     *   Optional. The data to sign.
     *   It can't be supplied if
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.digest|AsymmetricSignRequest.digest}
     *   is supplied.
     * @param {google.protobuf.Int64Value} [request.dataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.data|AsymmetricSignRequest.data}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.data|AsymmetricSignRequest.data}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.AsymmetricSignRequest.data|AsymmetricSignRequest.data})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.AsymmetricSignRequest.data_crc32c|AsymmetricSignRequest.data_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.AsymmetricSignResponse|AsymmetricSignResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.asymmetric_sign.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_AsymmetricSign_async
     */
    asymmetricSign(request?: protos.google.cloud.kms.v1.IAsymmetricSignRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IAsymmetricSignResponse,
        protos.google.cloud.kms.v1.IAsymmetricSignRequest | undefined,
        {} | undefined
    ]>;
    asymmetricSign(request: protos.google.cloud.kms.v1.IAsymmetricSignRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IAsymmetricSignResponse, protos.google.cloud.kms.v1.IAsymmetricSignRequest | null | undefined, {} | null | undefined>): void;
    asymmetricSign(request: protos.google.cloud.kms.v1.IAsymmetricSignRequest, callback: Callback<protos.google.cloud.kms.v1.IAsymmetricSignResponse, protos.google.cloud.kms.v1.IAsymmetricSignRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Decrypts data that was encrypted with a public key retrieved from
     * {@link protos.google.cloud.kms.v1.KeyManagementService.GetPublicKey|GetPublicKey}
     * corresponding to a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}
     * with {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose}
     * ASYMMETRIC_DECRYPT.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   decryption.
     * @param {Buffer} request.ciphertext
     *   Required. The data encrypted with the named
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}'s public key using
     *   OAEP.
     * @param {google.protobuf.Int64Value} [request.ciphertextCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.AsymmetricDecryptRequest.ciphertext|AsymmetricDecryptRequest.ciphertext}.
     *   If specified,
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.AsymmetricDecryptRequest.ciphertext|AsymmetricDecryptRequest.ciphertext}
     *   using this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.AsymmetricDecryptRequest.ciphertext|AsymmetricDecryptRequest.ciphertext})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.AsymmetricDecryptRequest.ciphertext_crc32c|AsymmetricDecryptRequest.ciphertext_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.AsymmetricDecryptResponse|AsymmetricDecryptResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.asymmetric_decrypt.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_AsymmetricDecrypt_async
     */
    asymmetricDecrypt(request?: protos.google.cloud.kms.v1.IAsymmetricDecryptRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IAsymmetricDecryptResponse,
        protos.google.cloud.kms.v1.IAsymmetricDecryptRequest | undefined,
        {} | undefined
    ]>;
    asymmetricDecrypt(request: protos.google.cloud.kms.v1.IAsymmetricDecryptRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IAsymmetricDecryptResponse, protos.google.cloud.kms.v1.IAsymmetricDecryptRequest | null | undefined, {} | null | undefined>): void;
    asymmetricDecrypt(request: protos.google.cloud.kms.v1.IAsymmetricDecryptRequest, callback: Callback<protos.google.cloud.kms.v1.IAsymmetricDecryptResponse, protos.google.cloud.kms.v1.IAsymmetricDecryptRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Signs data using a {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}
     * with {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} MAC,
     * producing a tag that can be verified by another source with the same key.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   signing.
     * @param {Buffer} request.data
     *   Required. The data to sign. The MAC tag is computed over this data field
     *   based on the specific algorithm.
     * @param {google.protobuf.Int64Value} [request.dataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.MacSignRequest.data|MacSignRequest.data}. If
     *   specified, {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.MacSignRequest.data|MacSignRequest.data} using this
     *   checksum. {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.MacSignRequest.data|MacSignRequest.data}) is
     *   equal to
     *   {@link protos.google.cloud.kms.v1.MacSignRequest.data_crc32c|MacSignRequest.data_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.MacSignResponse|MacSignResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.mac_sign.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_MacSign_async
     */
    macSign(request?: protos.google.cloud.kms.v1.IMacSignRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IMacSignResponse,
        protos.google.cloud.kms.v1.IMacSignRequest | undefined,
        {} | undefined
    ]>;
    macSign(request: protos.google.cloud.kms.v1.IMacSignRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IMacSignResponse, protos.google.cloud.kms.v1.IMacSignRequest | null | undefined, {} | null | undefined>): void;
    macSign(request: protos.google.cloud.kms.v1.IMacSignRequest, callback: Callback<protos.google.cloud.kms.v1.IMacSignResponse, protos.google.cloud.kms.v1.IMacSignRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Verifies MAC tag using a
     * {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} with
     * {@link protos.google.cloud.kms.v1.CryptoKey.purpose|CryptoKey.purpose} MAC, and returns
     * a response that indicates whether or not the verification was successful.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.name
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} to use for
     *   verification.
     * @param {Buffer} request.data
     *   Required. The data used previously as a
     *   {@link protos.google.cloud.kms.v1.MacSignRequest.data|MacSignRequest.data} to generate
     *   the MAC tag.
     * @param {google.protobuf.Int64Value} [request.dataCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.data|MacVerifyRequest.data}. If
     *   specified, {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.data|MacVerifyRequest.data} using
     *   this checksum.
     *   {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService} will
     *   report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.google.cloud.kms.v1.MacVerifyRequest.data|MacVerifyRequest.data})
     *   is equal to
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.data_crc32c|MacVerifyRequest.data_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {Buffer} request.mac
     *   Required. The signature to verify.
     * @param {google.protobuf.Int64Value} [request.macCrc32c]
     *   Optional. An optional CRC32C checksum of the
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.mac|MacVerifyRequest.mac}. If
     *   specified, {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will verify the integrity of the received
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.mac|MacVerifyRequest.mac} using this
     *   checksum. {@link protos.google.cloud.kms.v1.KeyManagementService|KeyManagementService}
     *   will report an error if the checksum verification fails. If you receive a
     *   checksum error, your client should verify that
     *   CRC32C({@link protos.|MacVerifyRequest.tag}) is equal to
     *   {@link protos.google.cloud.kms.v1.MacVerifyRequest.mac_crc32c|MacVerifyRequest.mac_crc32c},
     *   and if so, perform a limited number of retries. A persistent mismatch may
     *   indicate an issue in your computation of the CRC32C checksum. Note: This
     *   field is defined as int64 for reasons of compatibility across different
     *   languages. However, it is a non-negative integer, which will never exceed
     *   2^32-1, and can be safely downconverted to uint32 in languages that support
     *   this type.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.MacVerifyResponse|MacVerifyResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.mac_verify.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_MacVerify_async
     */
    macVerify(request?: protos.google.cloud.kms.v1.IMacVerifyRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IMacVerifyResponse,
        protos.google.cloud.kms.v1.IMacVerifyRequest | undefined,
        {} | undefined
    ]>;
    macVerify(request: protos.google.cloud.kms.v1.IMacVerifyRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IMacVerifyResponse, protos.google.cloud.kms.v1.IMacVerifyRequest | null | undefined, {} | null | undefined>): void;
    macVerify(request: protos.google.cloud.kms.v1.IMacVerifyRequest, callback: Callback<protos.google.cloud.kms.v1.IMacVerifyResponse, protos.google.cloud.kms.v1.IMacVerifyRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Generate random bytes using the Cloud KMS randomness source in the provided
     * location.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.location
     *   The project-specific location in which to generate random bytes.
     *   For example, "projects/my-project/locations/us-central1".
     * @param {number} request.lengthBytes
     *   The length in bytes of the amount of randomness to retrieve.  Minimum 8
     *   bytes, maximum 1024 bytes.
     * @param {google.cloud.kms.v1.ProtectionLevel} request.protectionLevel
     *   The {@link protos.google.cloud.kms.v1.ProtectionLevel|ProtectionLevel} to use when
     *   generating the random data. Currently, only
     *   {@link protos.google.cloud.kms.v1.ProtectionLevel.HSM|HSM} protection level is
     *   supported.
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is an object representing {@link protos.google.cloud.kms.v1.GenerateRandomBytesResponse|GenerateRandomBytesResponse}.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#regular-methods | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.generate_random_bytes.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_GenerateRandomBytes_async
     */
    generateRandomBytes(request?: protos.google.cloud.kms.v1.IGenerateRandomBytesRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IGenerateRandomBytesResponse,
        protos.google.cloud.kms.v1.IGenerateRandomBytesRequest | undefined,
        {} | undefined
    ]>;
    generateRandomBytes(request: protos.google.cloud.kms.v1.IGenerateRandomBytesRequest, options: CallOptions, callback: Callback<protos.google.cloud.kms.v1.IGenerateRandomBytesResponse, protos.google.cloud.kms.v1.IGenerateRandomBytesRequest | null | undefined, {} | null | undefined>): void;
    generateRandomBytes(request: protos.google.cloud.kms.v1.IGenerateRandomBytesRequest, callback: Callback<protos.google.cloud.kms.v1.IGenerateRandomBytesResponse, protos.google.cloud.kms.v1.IGenerateRandomBytesRequest | null | undefined, {} | null | undefined>): void;
    /**
     * Lists {@link protos.google.cloud.kms.v1.KeyRing|KeyRings}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings}, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} to include in the response. Further
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} can subsequently be obtained by
     *   including the
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}.
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
     *   The first element of the array is Array of {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed and will merge results from all the pages into this array.
     *   Note that it can affect your quota.
     *   We recommend using `listKeyRingsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listKeyRings(request?: protos.google.cloud.kms.v1.IListKeyRingsRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IKeyRing[],
        protos.google.cloud.kms.v1.IListKeyRingsRequest | null,
        protos.google.cloud.kms.v1.IListKeyRingsResponse
    ]>;
    listKeyRings(request: protos.google.cloud.kms.v1.IListKeyRingsRequest, options: CallOptions, callback: PaginationCallback<protos.google.cloud.kms.v1.IListKeyRingsRequest, protos.google.cloud.kms.v1.IListKeyRingsResponse | null | undefined, protos.google.cloud.kms.v1.IKeyRing>): void;
    listKeyRings(request: protos.google.cloud.kms.v1.IListKeyRingsRequest, callback: PaginationCallback<protos.google.cloud.kms.v1.IListKeyRingsRequest, protos.google.cloud.kms.v1.IListKeyRingsResponse | null | undefined, protos.google.cloud.kms.v1.IKeyRing>): void;
    /**
     * Equivalent to `method.name.toCamelCase()`, but returns a NodeJS Stream object.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings}, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} to include in the response. Further
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} can subsequently be obtained by
     *   including the
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}.
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
     *   An object stream which emits an object representing {@link protos.google.cloud.kms.v1.KeyRing|KeyRing} on 'data' event.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed. Note that it can affect your quota.
     *   We recommend using `listKeyRingsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listKeyRingsStream(request?: protos.google.cloud.kms.v1.IListKeyRingsRequest, options?: CallOptions): Transform;
    /**
     * Equivalent to `listKeyRings`, but returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the location associated with the
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings}, in the format
     *   `projects/* /locations/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} to include in the response. Further
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRings} can subsequently be obtained by
     *   including the
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListKeyRingsResponse.next_page_token|ListKeyRingsResponse.next_page_token}.
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
     *   {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.list_key_rings.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_ListKeyRings_async
     */
    listKeyRingsAsync(request?: protos.google.cloud.kms.v1.IListKeyRingsRequest, options?: CallOptions): AsyncIterable<protos.google.cloud.kms.v1.IKeyRing>;
    /**
     * Lists {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.versionView
     *   The fields of the primary version to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is Array of {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed and will merge results from all the pages into this array.
     *   Note that it can affect your quota.
     *   We recommend using `listCryptoKeysAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listCryptoKeys(request?: protos.google.cloud.kms.v1.IListCryptoKeysRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKey[],
        protos.google.cloud.kms.v1.IListCryptoKeysRequest | null,
        protos.google.cloud.kms.v1.IListCryptoKeysResponse
    ]>;
    listCryptoKeys(request: protos.google.cloud.kms.v1.IListCryptoKeysRequest, options: CallOptions, callback: PaginationCallback<protos.google.cloud.kms.v1.IListCryptoKeysRequest, protos.google.cloud.kms.v1.IListCryptoKeysResponse | null | undefined, protos.google.cloud.kms.v1.ICryptoKey>): void;
    listCryptoKeys(request: protos.google.cloud.kms.v1.IListCryptoKeysRequest, callback: PaginationCallback<protos.google.cloud.kms.v1.IListCryptoKeysRequest, protos.google.cloud.kms.v1.IListCryptoKeysResponse | null | undefined, protos.google.cloud.kms.v1.ICryptoKey>): void;
    /**
     * Equivalent to `method.name.toCamelCase()`, but returns a NodeJS Stream object.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.versionView
     *   The fields of the primary version to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Stream}
     *   An object stream which emits an object representing {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} on 'data' event.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed. Note that it can affect your quota.
     *   We recommend using `listCryptoKeysAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listCryptoKeysStream(request?: protos.google.cloud.kms.v1.IListCryptoKeysRequest, options?: CallOptions): Transform;
    /**
     * Equivalent to `listCryptoKeys`, but returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKeys} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}
     *   in a subsequent request.  If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeysResponse.next_page_token|ListCryptoKeysResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.versionView
     *   The fields of the primary version to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Object}
     *   An iterable Object that allows {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | async iteration }.
     *   When you iterate the returned iterable, each element will be an object representing
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.list_crypto_keys.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_ListCryptoKeys_async
     */
    listCryptoKeysAsync(request?: protos.google.cloud.kms.v1.IListCryptoKeysRequest, options?: CallOptions): AsyncIterable<protos.google.cloud.kms.v1.ICryptoKey>;
    /**
     * Lists {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to list, in the format
     *   `projects/* /locations/* /keyRings/* /cryptoKeys/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}
     *   can subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.view
     *   The fields to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is Array of {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed and will merge results from all the pages into this array.
     *   Note that it can affect your quota.
     *   We recommend using `listCryptoKeyVersionsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listCryptoKeyVersions(request?: protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.ICryptoKeyVersion[],
        protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest | null,
        protos.google.cloud.kms.v1.IListCryptoKeyVersionsResponse
    ]>;
    listCryptoKeyVersions(request: protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, options: CallOptions, callback: PaginationCallback<protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, protos.google.cloud.kms.v1.IListCryptoKeyVersionsResponse | null | undefined, protos.google.cloud.kms.v1.ICryptoKeyVersion>): void;
    listCryptoKeyVersions(request: protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, callback: PaginationCallback<protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, protos.google.cloud.kms.v1.IListCryptoKeyVersionsResponse | null | undefined, protos.google.cloud.kms.v1.ICryptoKeyVersion>): void;
    /**
     * Equivalent to `method.name.toCamelCase()`, but returns a NodeJS Stream object.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to list, in the format
     *   `projects/* /locations/* /keyRings/* /cryptoKeys/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}
     *   can subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.view
     *   The fields to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Stream}
     *   An object stream which emits an object representing {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion} on 'data' event.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed. Note that it can affect your quota.
     *   We recommend using `listCryptoKeyVersionsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listCryptoKeyVersionsStream(request?: protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, options?: CallOptions): Transform;
    /**
     * Equivalent to `listCryptoKeyVersions`, but returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the
     *   {@link protos.google.cloud.kms.v1.CryptoKey|CryptoKey} to list, in the format
     *   `projects/* /locations/* /keyRings/* /cryptoKeys/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions} to include in the
     *   response. Further {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersions}
     *   can subsequently be obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListCryptoKeyVersionsResponse.next_page_token|ListCryptoKeyVersionsResponse.next_page_token}.
     * @param {google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionView} request.view
     *   The fields to include in the response.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Object}
     *   An iterable Object that allows {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | async iteration }.
     *   When you iterate the returned iterable, each element will be an object representing
     *   {@link protos.google.cloud.kms.v1.CryptoKeyVersion|CryptoKeyVersion}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.list_crypto_key_versions.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_ListCryptoKeyVersions_async
     */
    listCryptoKeyVersionsAsync(request?: protos.google.cloud.kms.v1.IListCryptoKeyVersionsRequest, options?: CallOptions): AsyncIterable<protos.google.cloud.kms.v1.ICryptoKeyVersion>;
    /**
     * Lists {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs}.
     *
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Promise} - The promise which resolves to an array.
     *   The first element of the array is Array of {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed and will merge results from all the pages into this array.
     *   Note that it can affect your quota.
     *   We recommend using `listImportJobsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listImportJobs(request?: protos.google.cloud.kms.v1.IListImportJobsRequest, options?: CallOptions): Promise<[
        protos.google.cloud.kms.v1.IImportJob[],
        protos.google.cloud.kms.v1.IListImportJobsRequest | null,
        protos.google.cloud.kms.v1.IListImportJobsResponse
    ]>;
    listImportJobs(request: protos.google.cloud.kms.v1.IListImportJobsRequest, options: CallOptions, callback: PaginationCallback<protos.google.cloud.kms.v1.IListImportJobsRequest, protos.google.cloud.kms.v1.IListImportJobsResponse | null | undefined, protos.google.cloud.kms.v1.IImportJob>): void;
    listImportJobs(request: protos.google.cloud.kms.v1.IListImportJobsRequest, callback: PaginationCallback<protos.google.cloud.kms.v1.IListImportJobsRequest, protos.google.cloud.kms.v1.IListImportJobsResponse | null | undefined, protos.google.cloud.kms.v1.IImportJob>): void;
    /**
     * Equivalent to `method.name.toCamelCase()`, but returns a NodeJS Stream object.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Stream}
     *   An object stream which emits an object representing {@link protos.google.cloud.kms.v1.ImportJob|ImportJob} on 'data' event.
     *   The client library will perform auto-pagination by default: it will call the API as many
     *   times as needed. Note that it can affect your quota.
     *   We recommend using `listImportJobsAsync()`
     *   method described below for async iteration which you can stop as needed.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     */
    listImportJobsStream(request?: protos.google.cloud.kms.v1.IListImportJobsRequest, options?: CallOptions): Transform;
    /**
     * Equivalent to `listImportJobs`, but returns an iterable object.
     *
     * `for`-`await`-`of` syntax is used with the iterable to get response elements on-demand.
     * @param {Object} request
     *   The request object that will be sent.
     * @param {string} request.parent
     *   Required. The resource name of the {@link protos.google.cloud.kms.v1.KeyRing|KeyRing}
     *   to list, in the format `projects/* /locations/* /keyRings/*`.
     * @param {number} [request.pageSize]
     *   Optional. Optional limit on the number of
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} to include in the response.
     *   Further {@link protos.google.cloud.kms.v1.ImportJob|ImportJobs} can subsequently be
     *   obtained by including the
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}
     *   in a subsequent request. If unspecified, the server will pick an
     *   appropriate default.
     * @param {string} [request.pageToken]
     *   Optional. Optional pagination token, returned earlier via
     *   {@link protos.google.cloud.kms.v1.ListImportJobsResponse.next_page_token|ListImportJobsResponse.next_page_token}.
     * @param {string} [request.filter]
     *   Optional. Only include resources that match the filter in the response. For
     *   more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {string} [request.orderBy]
     *   Optional. Specify how the results should be sorted. If not specified, the
     *   results will be sorted in the default order. For more information, see
     *   [Sorting and filtering list
     *   results](https://cloud.google.com/kms/docs/sorting-and-filtering).
     * @param {object} [options]
     *   Call options. See {@link https://googleapis.dev/nodejs/google-gax/latest/interfaces/CallOptions.html|CallOptions} for more details.
     * @returns {Object}
     *   An iterable Object that allows {@link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols | async iteration }.
     *   When you iterate the returned iterable, each element will be an object representing
     *   {@link protos.google.cloud.kms.v1.ImportJob|ImportJob}. The API will be called under the hood as needed, once per the page,
     *   so you can stop the iteration when you don't need more results.
     *   Please see the {@link https://github.com/googleapis/gax-nodejs/blob/master/client-libraries.md#auto-pagination | documentation }
     *   for more details and examples.
     * @example <caption>include:samples/generated/v1/key_management_service.list_import_jobs.js</caption>
     * region_tag:cloudkms_v1_generated_KeyManagementService_ListImportJobs_async
     */
    listImportJobsAsync(request?: protos.google.cloud.kms.v1.IListImportJobsRequest, options?: CallOptions): AsyncIterable<protos.google.cloud.kms.v1.IImportJob>;
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
