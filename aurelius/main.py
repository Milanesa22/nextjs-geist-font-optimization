"""
Main entry point for AURELIUS autonomous backend system.
Initializes all modules and starts the system.
"""

import asyncio
import signal
import sys
from typing import Optional

from .logger import setup_logger, get_logger
from .config import settings
from .modules.data_store import initialize_data_store, close_data_store
from .modules.scheduler import start_scheduler, stop_scheduler
from .modules.social import discord

# Setup logging
setup_logger(settings.log_level, settings.log_file)
logger = get_logger("main")

class AureliusSystem:
    """Main AURELIUS system controller."""
    
    def __init__(self):
        self.running = False
        self.shutdown_event = asyncio.Event()
    
    async def start(self):
        """Start the AURELIUS system."""
        if self.running:
            logger.warning("AURELIUS system is already running")
            return
        
        logger.info("🚀 Starting AURELIUS Autonomous Backend System")
        logger.info("=" * 60)
        
        try:
            # Initialize data store
            logger.info("Initializing data store...")
            redis_available = await initialize_data_store()
            if redis_available:
                logger.info("✅ Redis connection established")
            else:
                logger.warning("⚠️  Using local storage fallback")
            
            # Send startup notification to Discord
            try:
                await discord.post_announcement(
                    "System Startup",
                    f"🚀 AURELIUS system is starting up...\n"
                    f"Data Store: {'Redis' if redis_available else 'Local Storage'}\n"
                    f"Configuration: {settings.paypal_mode.upper()} mode"
                )
            except Exception as e:
                logger.warning(f"Could not send startup notification: {str(e)}")
            
            # Start task scheduler
            logger.info("Starting task scheduler...")
            await start_scheduler()
            logger.info("✅ Task scheduler started")
            
            # System is now running
            self.running = True
            
            # Send startup complete notification
            try:
                await discord.post_announcement(
                    "System Ready",
                    "✅ AURELIUS system is now fully operational!\n\n"
                    "Active modules:\n"
                    "• Social Media Integration (Twitter, Mastodon, Discord)\n"
                    "• AI Content Generation (OpenAI GPT-4o)\n"
                    "• Sales Automation & Lead Management\n"
                    "• Payment Processing (PayPal)\n"
                    "• Analytics & Reporting\n"
                    "• Auto-Learning & Optimization\n"
                    "• Scheduled Task Management"
                )
            except Exception as e:
                logger.warning(f"Could not send ready notification: {str(e)}")
            
            logger.info("🎉 AURELIUS system started successfully!")
            logger.info("=" * 60)
            logger.info("System is now running. Press Ctrl+C to stop.")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
        except Exception as e:
            logger.critical(f"Failed to start AURELIUS system: {str(e)}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the AURELIUS system."""
        if not self.running:
            return
        
        logger.info("🛑 Stopping AURELIUS system...")
        
        try:
            # Send shutdown notification
            try:
                await discord.post_announcement(
                    "System Shutdown",
                    "🛑 AURELIUS system is shutting down gracefully..."
                )
            except Exception as e:
                logger.warning(f"Could not send shutdown notification: {str(e)}")
            
            # Stop task scheduler
            logger.info("Stopping task scheduler...")
            await stop_scheduler()
            logger.info("✅ Task scheduler stopped")
            
            # Close data store
            logger.info("Closing data store...")
            await close_data_store()
            logger.info("✅ Data store closed")
            
            self.running = False
            logger.info("✅ AURELIUS system stopped successfully")
            
        except Exception as e:
            logger.error(f"Error during system shutdown: {str(e)}")
        finally:
            self.shutdown_event.set()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.stop())

# Global system instance
aurelius_system = AureliusSystem()

async def main():
    """Main function to run AURELIUS system."""
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        aurelius_system.shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start the system
        await aurelius_system.start()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.critical(f"Critical error in main: {str(e)}")
        sys.exit(1)
    finally:
        # Ensure clean shutdown
        await aurelius_system.stop()

def run():
    """Entry point function for running AURELIUS."""
    try:
        # Check Python version
        if sys.version_info < (3, 8):
            print("ERROR: AURELIUS requires Python 3.8 or higher")
            sys.exit(1)
        
        # Print startup banner
        print_banner()
        
        # Run the async main function
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\n👋 AURELIUS shutdown complete. Goodbye!")
    except Exception as e:
        print(f"💥 Fatal error: {str(e)}")
        sys.exit(1)

def print_banner():
    """Print AURELIUS startup banner."""
    banner = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║     █████╗ ██╗   ██╗██████╗ ███████╗██╗     ██╗██╗   ██╗███████╗    ║
    ║    ██╔══██╗██║   ██║██╔══██╗██╔════╝██║     ██║██║   ██║██╔════╝    ║
    ║    ███████║██║   ██║██████╔╝█████╗  ██║     ██║██║   ██║███████╗    ║
    ║    ██╔══██║██║   ██║██╔══██╗██╔══╝  ██║     ██║██║   ██║╚════██║    ║
    ║    ██║  ██║╚██████╔╝██║  ██║███████╗███████╗██║╚██████╔╝███████║    ║
    ║    ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝ ╚═════╝ ╚══════╝    ║
    ║                                                               ║
    ║              Autonomous Backend for Business Management       ║
    ║                                                               ║
    ║    🤖 AI-Powered Social Media Automation                     ║
    ║    💰 Intelligent Sales & Lead Management                    ║
    ║    📊 Advanced Analytics & Auto-Learning                     ║
    ║    🔄 Multi-Platform Integration                             ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    print(f"    Version: 1.0.0")
    print(f"    Mode: {settings.paypal_mode.upper()}")
    print(f"    Log Level: {settings.log_level}")
    print()

# Health check endpoint function (for monitoring)
async def health_check() -> dict:
    """
    Health check function for monitoring systems.
    
    Returns:
        Health status dictionary
    """
    try:
        from .modules.data_store import data_store
        
        # Check if system is running
        if not aurelius_system.running:
            return {
                "status": "down",
                "message": "System is not running",
                "timestamp": asyncio.get_event_loop().time()
            }
        
        # Check data store
        try:
            await data_store.set("health_ping", "ok", expire=60)
            data_store_ok = True
        except:
            data_store_ok = False
        
        # Get system health from data store
        system_health = await data_store.get("system_health") or {}
        
        overall_status = "healthy"
        if not data_store_ok:
            overall_status = "degraded"
        elif system_health.get("overall_status") == "unhealthy":
            overall_status = "unhealthy"
        
        return {
            "status": overall_status,
            "components": {
                "data_store": "ok" if data_store_ok else "error",
                "scheduler": "ok" if aurelius_system.running else "error"
            },
            "system_health": system_health,
            "timestamp": asyncio.get_event_loop().time()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": asyncio.get_event_loop().time()
        }

if __name__ == "__main__":
    run()
