import * as https from "https";

export class TapisV3Auth {
    private authRefresh: NodeJS.Timeout | undefined;
    private auth!: Promise<string>;
    private username: string;
    private password: string;
    private tenantURL: string;

    constructor(username: string, password: string, tenantURL: string) {
        this.username = username;
        this.password = password;
        this.tenantURL = tenantURL;
        this.authenticate();
    }

    private authenticate(): void {
        // Construct the authentication URL
        const authUrl = `${this.tenantURL}/v3/oauth2/tokens`;

        // Construct the payload for authentication
        const authPayload = `username=${encodeURIComponent(this.username)}&password=${encodeURIComponent(this.password)}&grant_type=password&scope=user`;

        // Set options for the authentication request
        const authOptions = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            rejectUnauthorized: false,
            requestCert: true
        };

        // set auth promise to authentication funct
        this.auth = new Promise((resolve, reject) => {
            // Initiate the authentication request
            const authReq = https.request(authUrl, authOptions, (authRes) => {
                authRes.setEncoding('utf8');
                let authData = '';

                // Accumulate response data
                authRes.on('data', (chunk) => {
                    authData += chunk;
                });

                // When response is complete
                authRes.on('end', () => {
                    try {
                        const parsedResponse = JSON.parse(authData);
                        // If authentication is successful
                        if(authRes.statusCode === 200 && parsedResponse.result?.access_token?.access_token) {
                            //reauth one minute before token goes stale
                            this.authRefresh = setTimeout(() => {
                                this.authenticate();
                            }, (parsedResponse.result.access_token.expires_in - 60) * 1000);
                            resolve(parsedResponse.result.access_token.access_token);
                        }
                        else {
                            throw new Error('Authentication failed');
                        }
                    }
                    catch (error) {
                        reject(error);
                    }
                });
            });

            // Handle errors in the authentication request
            authReq.on('error', (error) => {
                reject(error);
            });

            // Send authentication payload
            authReq.write(authPayload);
            authReq.end();
        });
    }

    public async getToken(): Promise<string> {
        return this.auth;
    }

    public close(): void {
        clearTimeout(this.authRefresh);
    }
}