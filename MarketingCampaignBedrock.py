import boto3
import json
import base64

def read_file_from_s3(bucket_name, file_key):
    """
    Read content from an S3 file
    """
    try:
        s3_client = boto3.client('s3', region_name='us-west-2')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        print("\n" + "Retrieved text content from S3 file: " + file_key.split("/")[-1] + "\n")
        return file_content

    except Exception as e:
        print(f"Error reading from S3: {str(e)}")
        raise

def generate_compelling_text(prompt_text):
    """
    Invoke Bedrock model with text prompt to generate compelling text Personal Hook.
    """
    try:
        # Initialize the Bedrock Runtime client
        bedrock_runtime = boto3.client(
            service_name='bedrock-runtime',
            region_name='us-east-1'  # Replace with your region
        )
        
        # Specify Bedrock model Id 
        model_id = 'us.amazon.nova-pro-v1:0'
        
        # Define your system prompt(s).
        system_list = [
            {
                "text": "I can write compelling text for marketing content."
            }
        ]

        # Define one or more messages using the "user" and "assistant" roles.
        message_list = [{"role": "user", "content": [{"text": prompt_text}]}]
       
        # Configure the inference parameters.
        inf_params = {"maxTokens": 7000, "topP": 0.9, "topK": 20, "temperature": 0.7}

        request_body = {
            "schemaVersion": "messages-v1",
            "messages": message_list,
            "system": system_list,
            "inferenceConfig": inf_params,
            }

        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body = json.dumps(request_body)
        )

        # Parse and return the response
        response_body = json.loads(response['body'].read())
        print("Generated compelling text " + "\n")
        return response_body

    except Exception as e:
        print(f"Error invoking Bedrock: {str(e)}")
        raise

def generate_marketing_images(image_prompt, product_name):
    """
    Invoke Bedrock model with image prompt to generate marketing images.
    """
    image_prompt_template = """UNIVERSAL E-COMMERCE VISUAL TEMPLATE
    [PRODUCT NAME]: {}

    LIFESTYLE SCENE:
    Environment Setting:
    Appropriate room/space for product category
    Well-organized display area
    Clean, uncluttered spaces
    Color palette: bright

    PRODUCT SHOT:
    Primary Composition:
    Main product positioned at dynamic 45-degree angle
    Product-specific styling elements
    Key feature highlight
    Complementary items relevant to category

    Supporting Elements:
    Natural lighting appropriate to setting
    Strategic shadows for depth
    Category-specific accessories

    Style Specifications:
    Contemporary minimalist aesthetic
    High-key lighting with soft shadows
    Clean, uncluttered look

    Technical Details:
    High resolution, sharp details
    Rich, natural color saturation
    Balanced composition following rule of thirds
    Subtle vignetting to draw eye to product
    Professional product photography techniques"""

    image_prompt = image_prompt_template.format(product_name)

    try:
        # Initialize the Bedrock Runtime client
        bedrock_runtime = boto3.client(
            'bedrock-runtime', 
            region_name='us-east-1'  # Replace with youregion_name='us-east-1'
            )
        
        # Specify Bedrock model Id 
        model_id = "amazon.nova-canvas-v1:0"

        # Define Request parameters 
        request_body = {
            "taskType": "TEXT_IMAGE",
            "textToImageParams": {
                "text": image_prompt,
                "negativeText": "blurry, low quality"
            },
            "imageGenerationConfig": {
                "numberOfImages": 3,
                "quality": "standard",
                "height": 1024,
                "width": 1024,
                "cfgScale": 8.0
            }
        }

        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body),
        )

        # Parse and return the response
        response_body = json.loads(response['body'].read())

        image_data1 = response_body['images'][0]
        image_data2 = response_body['images'][1]
        image_data3 = response_body['images'][2]

        image_bytes1 = base64.b64decode(image_data1)
        image_bytes2 = base64.b64decode(image_data2)
        image_bytes3 = base64.b64decode(image_data3)

        # Write iamges to files
        with open('generated_image1.png', 'wb') as f1:
            f1.write(image_bytes1)
        with open('generated_image2.png', 'wb') as f2:
            f2.write(image_bytes2)
        with open('generated_image3.png', 'wb') as f3:
            f3.write(image_bytes3)

        print("Generated images " + f1.name + " " + f2.name + " " + f3.name + "\n")
        return f1.name, f2.name, f3.name
    
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise

def generate_marketing_html(compelling_text: str, images: tuple):
    """
    Generate marketing HTML using Bedrock model.
    """

    # Create prompt inputs for Bedrock model invoke.
    sys_prompt_text = "You are an experienced web developer. Your job is to produce a clean, professional, static HTML page to display the ##MARKETING CAMPAIGN CONTENT## provided below. Include placeholders for {}, {} and {} interspersed throughout the HTML page with a maximum image width of 50%.".format(images[0], images[1], images[2])
    system_list = [{"text": sys_prompt_text}]
    marketing_prompt_header = """##MARKETING CAMPAIGN CONTENT##
    ### Personalized Marketing Content for Working People

    """
    compelling_text_prompt = marketing_prompt_header + compelling_text["output"]["message"]["content"][0]["text"]
    message_list = [{"role": "user", "content": [{"text": compelling_text_prompt}]}]
    inf_params = {"maxTokens": 10240, "topP": 0.9, "topK": 20, "temperature": 0.7}

    request_body = {
        "schemaVersion": "messages-v1",
        "messages": message_list,
        "system": system_list,
        "inferenceConfig": inf_params,
    }

    try:
        bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

        model_id = "us.amazon.nova-pro-v1:0" # Or the specific version you are using

        response = bedrock_runtime.invoke_model(
            modelId=model_id,
            body=json.dumps(request_body)
        )

        # Parse the response and write HTML file.
        generated_text = json.loads(response['body'].read())["output"]["message"]["content"][0]["text"]

        with (open("Campaign.HTML", "w")) as f:
            f.write(generated_text.strip("`"))
        return f.name

    except Exception as e:
        print(f"Error invoking Bedrock: {str(e)}")
        raise

def main():
    # Get configuration from environment variables
    #bucket_name = os.environ.get('S3_BUCKET_NAME')
    #file_key = os.environ.get('S3_FILE_KEY')
    
    bucket_name = 'amazon-sagemaker-848577535453-us-east-1-3219376951c4'
    file_key = 'dzd_c1jky5s612n61z/bgs0qmu7awvfzb/dev/file123.txt'

    if not bucket_name or not file_key:
        raise ValueError("Please set S3_BUCKET_NAME and S3_FILE_KEY environment variables")

    try:
        # Read content from S3 and generate prompt text and extract product name.
        prompt_text = read_file_from_s3(bucket_name, file_key)
        pn_idx = prompt_text.find("Product Name")
        product_name = prompt_text[pn_idx:].split(":")[1].strip().split('\n')[0]
    
        # Generate Compelling text Personal Hook using Bedrock 
        compelling_text = generate_compelling_text(prompt_text)

        # Generate Marketing images using Bedrock
        imgaes = generate_marketing_images(prompt_text, product_name)
        
        # Generate campaining HTML
        campaign_file = generate_marketing_html(compelling_text, imgaes)
        print("Marketing Campaign HTML file is saved as: " + campaign_file + "\n")
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()

